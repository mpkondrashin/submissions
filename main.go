package main

import (
	"context"
	"embed"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/data/binding"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"
	"github.com/google/uuid"
	"github.com/mpkondrashin/ddan"
	"github.com/mpkondrashin/ddan/report/report27"
	"github.com/zalando/go-keyring"
)

const (
	keyringService  = "submissions-ddan"
	keyringUser     = "api-key"
	keyringUUID     = "client-uuid"
	keyringUUIDOld  = "client-uuid_OLD"
	prefAnalyzerURL = "analyzer_url"
	prefClientUUID  = "client_uuid"
	prefIgnoreTLS   = "ignore_tls"
	prefOutputDir   = "output_dir"

	apiCallInterval     = 10 * time.Millisecond
	getReportGoroutines = 10                     // concurrent workers for SampleInfo/GetReport; higher = faster, but more load on analyzer and local CPU
	apiLogEvery         = uint64(1)              // log every N-th API call (1 = log each call); increase to reduce log volume
	apiLogMinInterval   = 500 * time.Millisecond // minimum interval between API log lines even if apiLogEvery would allow more
	apiCallTimeout      = 2 * time.Minute        // per API call timeout; prevents a single hung request from stalling the whole export
	stallTimeout        = 1 * time.Minute        // abort export if no SRID completes / rows are written for this long (covers SDK/network hangs that ignore context)

	progressConnect    = 0.02 // progress after connecting starts; should be < progressListStart
	progressListStart  = 0.05 // progress when "Fetching submission list" starts; should be > progressConnect and <= progressDoneStart
	progressQueueSpan  = 0.05 // additional progress allocated to queuing SRIDs; progressListStart+progressQueueSpan should be <= progressDoneStart
	progressDoneStart  = 0.10 // base progress for per-SRID completion phase; should be >= progressListStart+progressQueueSpan
	progressDoneSpan   = 0.85 // additional progress allocated to per-SRID downloads; progressDoneStart+progressDoneSpan should be < progressFinalizing
	progressFinalizing = 0.95 // progress during final flush/cleanup; should be > progressDoneStart+progressDoneSpan and < progressComplete
	progressComplete   = 1.0  // progress value for completion; should be 1.0

	sourceID   = "303"
	sourceName = "Submissions"

	WindowWidth  = 640
	WindowHeight = 500
)

//go:embed images/*.png
var embeddedImagesFS embed.FS

type WizardApp struct {
	app           fyne.App
	window        fyne.Window
	currentScreen int
	analyzerURL   string
	ignoreTLS     bool
	apiKey        string
	verbose       bool
	clientUUID    string
	outputDir     string
	outputPath    string
	outputName    string
	startDate     string
	endDate       string
	logPath       string
	logFile       *os.File

	mu           sync.Mutex
	activeClient *ddan.Client
	registered   bool
}

type linkLabel struct {
	*widget.Hyperlink
	onTapped func()
}

func newLinkLabel(text string, onTapped func()) *linkLabel {
	l := &linkLabel{
		Hyperlink: widget.NewHyperlink(text, nil),
		onTapped:  onTapped,
	}
	return l
}

func (l *linkLabel) Tapped(_ *fyne.PointEvent) {
	if l.onTapped != nil {
		l.onTapped()
	}
}

func addFlattenSchemaKeys(prefix string, t reflect.Type, out map[string]struct{}, opts flattenOptions) {
	if t == nil {
		return
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
		if t == nil {
			return
		}
	}

	switch t.Kind() {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" {
				continue
			}
			key := prefix + "_" + strings.ToLower(f.Name)
			addFlattenSchemaKeys(key, f.Type, out, opts)
		}
		return
	case reflect.Slice, reflect.Array, reflect.Map:
		if !opts.IncludeLists {
			return
		}
		if _, ok := opts.IgnoreListFieldKeys[prefix]; ok {
			return
		}
		out[prefix] = struct{}{}
		return
	case reflect.Interface:
		out[prefix] = struct{}{}
		return
	default:
		out[prefix] = struct{}{}
		return
	}
}

func buildCSVHeader() []string {
	keysSet := make(map[string]struct{})
	keysSet["file_analyze_report_len"] = struct{}{}
	keysSet["download_error"] = struct{}{}

	addFlattenSchemaKeys("sample", reflect.TypeOf(ddan.SampleInfo{}), keysSet, flattenOptions{
		IncludeLists: true,
		IgnoreListFieldKeys: map[string]struct{}{
			"sample_attachments": {},
			"attachments":        {},
		},
	})
	addFlattenSchemaKeys("file", reflect.TypeOf(report27.FILEANALYZEREPORT{}), keysSet, flattenOptions{IncludeLists: false})

	keys := make([]string, 0, len(keysSet))
	for k := range keysSet {
		if k == "srid" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	header := make([]string, 0, 1+len(keys))
	header = append(header, "srid")
	header = append(header, keys...)
	return header
}

func reportRowToMap(r reportRow) map[string]string {
	m := map[string]string{
		"srid":                    r.SRID,
		"file_analyze_report_len": fmt.Sprintf("%d", r.FileAnalyzeReportLen),
		"download_error":          r.DownloadError,
	}
	if r.SampleInfo != nil {
		flattenFieldsWithOptions("sample", reflect.ValueOf(r.SampleInfo), m, flattenOptions{
			IncludeLists: true,
			IgnoreListFieldKeys: map[string]struct{}{
				"sample_attachments": {},
				"attachments":        {},
			},
		})
	}
	if r.FileAnalyzeReport != nil {
		flattenFieldsWithOptions("file", reflect.ValueOf(r.FileAnalyzeReport), m, flattenOptions{IncludeLists: false})
	}
	return m
}

func (w *WizardApp) stepHeader(step int, titleText, explanationText string) fyne.CanvasObject {
	resName := fmt.Sprintf("image_%d.png", step+1)
	b, err := embeddedImagesFS.ReadFile("images/" + resName)
	if err != nil {
		t := widget.NewLabelWithStyle(titleText, fyne.TextAlignCenter, fyne.TextStyle{Bold: true})
		ex := widget.NewLabel(explanationText)
		ex.Wrapping = fyne.TextWrapWord
		return container.NewVBox(t, ex)
	}

	imgRes := fyne.NewStaticResource(resName, b)
	img := canvas.NewImageFromResource(imgRes)
	img.FillMode = canvas.ImageFillContain
	img.SetMinSize(fyne.NewSize(160, 160))

	t := widget.NewLabelWithStyle(titleText, fyne.TextAlignCenter, fyne.TextStyle{Bold: true})
	ex := widget.NewLabel(explanationText)
	ex.Wrapping = fyne.TextWrapWord

	imgCol := container.NewVBox(img, layout.NewSpacer())
	textCol := container.NewVBox(ex, layout.NewSpacer())
	row := container.NewBorder(nil, nil, imgCol, nil,
		container.NewVBox(layout.NewSpacer(), textCol, layout.NewSpacer()),
	)
	return container.NewVBox(t, row)
}

func (w *WizardApp) defaultOutputName() string {
	from := strings.ReplaceAll(w.startDate, "-", "")
	to := strings.ReplaceAll(w.endDate, "-", "")
	if len(from) == 8 && len(to) == 8 {
		return fmt.Sprintf("submissions_%s_%s.csv", from, to)
	}
	return "submissions.csv"
}

func (w *WizardApp) loadAPIKey() {
	key, err := keyring.Get(keyringService, keyringUser)
	if err == nil {
		w.apiKey = key
	}
}

func (w *WizardApp) loadPrefs() {
	if w.app == nil {
		return
	}
	w.analyzerURL = strings.TrimSpace(w.app.Preferences().String(prefAnalyzerURL))
	w.ignoreTLS = w.app.Preferences().Bool(prefIgnoreTLS)
	w.outputDir = strings.TrimSpace(w.app.Preferences().String(prefOutputDir))
}

func (w *WizardApp) saveAnalyzerURL(u string) {
	if w.app == nil {
		return
	}
	w.app.Preferences().SetString(prefAnalyzerURL, strings.TrimSpace(u))
}

func (w *WizardApp) saveIgnoreTLS(v bool) {
	if w.app == nil {
		return
	}
	w.app.Preferences().SetBool(prefIgnoreTLS, v)
}

func (w *WizardApp) saveOutputDir(dir string) {
	if w.app == nil {
		return
	}
	w.app.Preferences().SetString(prefOutputDir, strings.TrimSpace(dir))
}

func (w *WizardApp) ensureClientUUID() string {
	if w.app == nil {
		u := strings.TrimSpace(w.clientUUID)
		if u != "" {
			return u
		}
		stored, err := keyring.Get(keyringService, keyringUUID)
		if err == nil {
			stored = strings.TrimSpace(stored)
			if stored != "" {
				w.clientUUID = stored
				return stored
			}
		}
		w.clientUUID = uuid.NewString()
		_ = keyring.Set(keyringService, keyringUUID, w.clientUUID)
		return w.clientUUID
	}
	stored, err := keyring.Get(keyringService, keyringUUID)
	if err == nil {
		stored = strings.TrimSpace(stored)
		if stored != "" {
			return stored
		}
	}
	newID := uuid.NewString()
	_ = keyring.Set(keyringService, keyringUUID, newID)
	return newID
}

func (w *WizardApp) rotateClientUUIDForRegister(forceUUID string) (newUUID, oldUUID string, err error) {
	forceUUID = strings.TrimSpace(forceUUID)
	if forceUUID != "" {
		_ = keyring.Delete(keyringService, keyringUUIDOld)
		if err := keyring.Set(keyringService, keyringUUID, forceUUID); err != nil {
			return "", "", err
		}
		return forceUUID, "", nil
	}

	cur, err := keyring.Get(keyringService, keyringUUID)
	if err == nil {
		cur = strings.TrimSpace(cur)
		if cur != "" {
			oldUUID = cur
			_ = keyring.Set(keyringService, keyringUUIDOld, cur)
		}
	}

	newUUID = uuid.NewString()
	if err := keyring.Set(keyringService, keyringUUID, newUUID); err != nil {
		return "", "", err
	}
	return newUUID, oldUUID, nil
}

func (w *WizardApp) deleteStoredClientUUIDs() {
	_ = keyring.Delete(keyringService, keyringUUID)
	_ = keyring.Delete(keyringService, keyringUUIDOld)
}

func (w *WizardApp) saveAPIKey(key string) error {
	return keyring.Set(keyringService, keyringUser, key)
}

func (w *WizardApp) ensureDefaultDates() {
	if strings.TrimSpace(w.endDate) == "" {
		w.endDate = time.Now().Format("2006-01-02")
	}
	if strings.TrimSpace(w.startDate) == "" {
		w.startDate = time.Now().AddDate(0, 0, -30).Format("2006-01-02")
	}
}

func defaultDownloadsDir() string {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return ""
	}
	return filepath.Join(home, "Downloads")
}

func main() {
	cli := flag.Bool("cli", false, "run in CLI mode (no GUI)")
	verbose := flag.Bool("verbose", false, "enable verbose DDAn SDK logging")
	cliAnalyzerURL := flag.String("analyzer-url", "", "DDAn analyzer URL (e.g. https://ddan.company.local)")
	cliIgnoreTLS := flag.Bool("ignore-tls", false, "ignore TLS verification errors")
	cliStart := flag.String("start", "", "start date (YYYY-MM-DD)")
	cliEnd := flag.String("end", "", "end date (YYYY-MM-DD)")
	cliOutput := flag.String("output", "", "output CSV file path")
	cliUUID := flag.String("uuid", "", "client UUID (optional)")
	cliLog := flag.String("log", "", "log file path (default: submissions_<date>.log next to executable)")
	flag.Parse()

	if *cli {
		w := &WizardApp{}
		w.verbose = *verbose
		w.analyzerURL = strings.TrimSpace(*cliAnalyzerURL)
		w.ignoreTLS = *cliIgnoreTLS
		w.startDate = strings.TrimSpace(*cliStart)
		w.endDate = strings.TrimSpace(*cliEnd)
		w.outputPath = strings.TrimSpace(*cliOutput)
		w.clientUUID = strings.TrimSpace(*cliUUID)
		w.logPath = strings.TrimSpace(*cliLog)
		w.ensureDefaultDates()
		w.initLogging()
		defer w.closeLogging()

		apiKey := strings.TrimSpace(os.Getenv("ANALYZER_API_KEY"))
		if apiKey == "" {
			log.Printf("error: ANALYZER_API_KEY env var is required")
			os.Exit(2)
		}
		w.apiKey = apiKey

		if w.analyzerURL == "" {
			log.Printf("error: --analyzer-url is required")
			os.Exit(2)
		}
		if w.outputPath == "" {
			log.Printf("error: --output is required")
			os.Exit(2)
		}
		if ext := strings.ToLower(path.Ext(w.outputPath)); ext != ".csv" {
			log.Printf("error: --output must end with .csv")
			os.Exit(2)
		}

		ctx := context.Background()
		if err := w.runExport(ctx, func(_ float64) {}, func(s string) { log.Printf("status: %s", s) }); err != nil {
			log.Printf("error: %v", err)
			os.Exit(1)
		}
		log.Printf("done: %s", w.outputPath)
		return
	}

	wizardApp := &WizardApp{}
	wizardApp.verbose = *verbose
	wizardApp.logPath = strings.TrimSpace(*cliLog)
	wizardApp.app = app.NewWithID("com.trendmicro.ddan.submissions")
	wizardApp.app.SetIcon(nil)
	wizardApp.window = wizardApp.app.NewWindow("Trend Micro DDAn Submissions Downloader")
	wizardApp.window.Resize(fyne.NewSize(WindowWidth, WindowHeight))
	wizardApp.window.SetFixedSize(true)
	wizardApp.window.CenterOnScreen()
	wizardApp.initLogging()
	wizardApp.loadPrefs()

	wizardApp.window.SetCloseIntercept(func() {
		log.Printf("window close intercept")
		wizardApp.bestEffortUnregister()
		wizardApp.closeLogging()
		wizardApp.window.Close()
	})

	wizardApp.showIntroScreen()

	wizardApp.window.ShowAndRun()
}

func (w *WizardApp) initLogging() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	exeDir := w.exeDir()
	_ = cleanupOldLogs(exeDir, 30*24*time.Hour)

	if strings.TrimSpace(w.logPath) == "" {
		w.logPath = filepath.Join(exeDir, fmt.Sprintf("submissions_%s.log", time.Now().Format("2006-01-02")))
	}
	if err := os.MkdirAll(filepath.Dir(w.logPath), 0o755); err != nil {
		log.Printf("error: create log dir: %v", err)
		return
	}
	f, err := os.OpenFile(w.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Printf("error: open log file %q: %v", w.logPath, err)
		return
	}
	w.logFile = f
	os.Stdout = f
	os.Stderr = f
	log.SetOutput(f)
	log.Printf("logging started: %s", w.logPath)
}

func (w *WizardApp) closeLogging() {
	if w.logFile == nil {
		return
	}
	_ = w.logFile.Close()
	w.logFile = nil
}

func (w *WizardApp) exeDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	if exe == "" {
		return "."
	}
	return filepath.Dir(exe)
}

func cleanupOldLogs(dir string, maxAge time.Duration) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-maxAge)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "submissions_") || !strings.HasSuffix(name, ".log") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		_ = os.Remove(filepath.Join(dir, name))
	}
	return nil
}

func (w *WizardApp) bestEffortUnregister() {
	w.mu.Lock()
	client := w.activeClient
	registered := w.registered
	w.mu.Unlock()

	if client == nil || !registered {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Unregister(ctx); err == nil {
		w.deleteStoredClientUUIDs()
	}
}

func (w *WizardApp) showIntroScreen() {
	w.currentScreen = 0
	log.Printf("screen: intro")

	header := w.stepHeader(0,
		"Trend Micro Deep Discovery Analyzer",
		"This application will help you download submission data from Trend Micro Deep Discovery Analyzer and export it to a CSV file.")

	subtitle := container.NewVBox(
		layout.NewSpacer(),
		widget.NewLabelWithStyle("Submissions Downloader", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		//layout.NewSpacer(),
	)

	continueBtn := widget.NewButton("Continue", func() {
		w.showAPIKeyScreen()
	})
	continueBtn.Importance = widget.HighImportance

	buttonBar := container.NewVBox(widget.NewSeparator(),
		container.NewPadded(container.NewHBox(layout.NewSpacer(), continueBtn)),
	)

	content := container.NewVBox(
		header,
		subtitle,
		//widget.NewSeparator(),
	)

	scrollContainer := container.NewScroll(container.NewPadded(content))
	w.window.SetContent(container.NewPadded(container.NewBorder(nil, buttonBar, nil, nil, scrollContainer)))
}

type reportRow struct {
	SRID                 string
	SampleInfo           any
	FileAnalyzeReport    *report27.FILEANALYZEREPORT
	FileAnalyzeReportLen int
	DownloadError        string
}

func xmlOrFieldName(sf reflect.StructField) string {
	tag := sf.Tag.Get("xml")
	if tag != "" {
		name := strings.TrimSpace(strings.Split(tag, ",")[0])
		if name != "" && name != "-" {
			return strings.ToLower(name)
		}
	}
	return strings.ToLower(sf.Name)
}

type flattenOptions struct {
	IncludeLists        bool
	IgnoreListFieldKeys map[string]struct{}
}

func flattenFields(prefix string, v reflect.Value, out map[string]string) {
	flattenFieldsWithOptions(prefix, v, out, flattenOptions{IncludeLists: true})
}

func flattenFieldsWithOptions(prefix string, v reflect.Value, out map[string]string, opts flattenOptions) {
	if !v.IsValid() {
		return
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		flattenFieldsWithOptions(prefix, v.Elem(), out, opts)
		return
	}

	switch v.Kind() {
	case reflect.Struct:
		if v.Type() == reflect.TypeFor[xml.Name]() {
			return
		}
		for i := 0; i < v.NumField(); i++ {
			sf := v.Type().Field(i)
			if !sf.IsExported() {
				continue
			}
			fv := v.Field(i)
			name := xmlOrFieldName(sf)
			key := name
			if prefix != "" {
				key = prefix + "_" + name
			}
			flattenFieldsWithOptions(key, fv, out, opts)
		}
		return
	case reflect.Slice, reflect.Array, reflect.Map:
		if prefix == "" {
			return
		}
		if opts.IgnoreListFieldKeys != nil {
			if _, ok := opts.IgnoreListFieldKeys[strings.ToLower(prefix)]; ok {
				return
			}
		}
		if !opts.IncludeLists {
			return
		}
		if !v.CanInterface() {
			return
		}
		b, err := json.Marshal(v.Interface())
		if err != nil {
			out[prefix] = fmt.Sprintf("%v", v.Interface())
			return
		}
		out[prefix] = string(b)
		return
	case reflect.Bool:
		out[prefix] = fmt.Sprintf("%t", v.Bool())
		return
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		out[prefix] = fmt.Sprintf("%d", v.Int())
		return
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		out[prefix] = fmt.Sprintf("%d", v.Uint())
		return
	case reflect.Float32, reflect.Float64:
		out[prefix] = fmt.Sprintf("%v", v.Float())
		return
	case reflect.String:
		out[prefix] = v.String()
		return
	case reflect.Interface:
		if v.IsNil() {
			return
		}
		flattenFieldsWithOptions(prefix, v.Elem(), out, opts)
		return
	default:
		return
	}
}

func (w *WizardApp) generateCSV(rows []reportRow) error {
	if err := os.MkdirAll(filepath.Dir(w.outputPath), 0o755); err != nil {
		return err
	}

	f, err := os.Create(w.outputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := csv.NewWriter(f)

	flatRows := make([]map[string]string, 0, len(rows))
	keysSet := make(map[string]struct{})
	for _, r := range rows {
		m := map[string]string{
			"srid":                    r.SRID,
			"file_analyze_report_len": fmt.Sprintf("%d", r.FileAnalyzeReportLen),
		}
		if r.SampleInfo != nil {
			flattenFieldsWithOptions("sample", reflect.ValueOf(r.SampleInfo), m, flattenOptions{
				IncludeLists: true,
				IgnoreListFieldKeys: map[string]struct{}{
					"sample_attachments": {},
					"attachments":        {},
				},
			})
		}
		if r.FileAnalyzeReport != nil {
			flattenFieldsWithOptions("file", reflect.ValueOf(r.FileAnalyzeReport), m, flattenOptions{IncludeLists: false})
		}
		flatRows = append(flatRows, m)
		for k := range m {
			if k == "srid" {
				continue
			}
			keysSet[k] = struct{}{}
		}
	}

	keys := make([]string, 0, len(keysSet))
	for k := range keysSet {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	header := make([]string, 0, 1+len(keys))
	header = append(header, "srid")
	header = append(header, keys...)
	if err := writer.Write(header); err != nil {
		return err
	}

	for _, m := range flatRows {
		rec := make([]string, 0, len(header))
		for _, k := range header {
			rec = append(rec, m[k])
		}
		if err := writer.Write(rec); err != nil {
			return err
		}
	}

	writer.Flush()
	return writer.Error()
}

func openFile(path string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("open", path).Start()
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", path).Start()
	default:
		return exec.Command("xdg-open", path).Start()
	}
}

func (w *WizardApp) showAPIKeyScreen() {
	w.currentScreen = 1
	log.Printf("screen: api_config")
	w.loadAPIKey()

	header := w.stepHeader(1,
		"API Key Configuration",
		"Please enter your Trend Micro Deep Discovery Analyzer API URL and API key.")

	analyzerURLEntry := widget.NewEntry()
	analyzerURLEntry.SetPlaceHolder("Analyzer URL (e.g. https://ddan.company.local)")
	analyzerURLEntry.SetText(w.analyzerURL)

	apiKeyEntry := widget.NewPasswordEntry()
	apiKeyEntry.SetPlaceHolder("Enter your API key...")
	apiKeyEntry.SetText(w.apiKey)

	ignoreTLSCheck := widget.NewCheck("Ignore TLS Errors", func(checked bool) {
		w.ignoreTLS = checked
		w.saveIgnoreTLS(w.ignoreTLS)
	})
	ignoreTLSCheck.SetChecked(w.ignoreTLS)

	continueBtn := widget.NewButton("Continue", func() {
		if strings.TrimSpace(analyzerURLEntry.Text) != "" && strings.TrimSpace(apiKeyEntry.Text) != "" {
			w.analyzerURL = strings.TrimSpace(analyzerURLEntry.Text)
			w.saveAnalyzerURL(w.analyzerURL)
			w.apiKey = strings.TrimSpace(apiKeyEntry.Text)
			_ = w.saveAPIKey(w.apiKey)
			w.saveIgnoreTLS(w.ignoreTLS)
			log.Printf("api_config saved: analyzer_url=%q ignore_tls=%v", w.analyzerURL, w.ignoreTLS)
			w.showTimeIntervalScreen()
		}
	})
	continueBtn.Importance = widget.HighImportance

	backBtn := widget.NewButton("Back", func() {
		w.showIntroScreen()
	})
	buttonBar := container.NewVBox(widget.NewSeparator(),
		container.NewPadded(container.NewHBox(backBtn, layout.NewSpacer(), continueBtn)),
	)

	content := container.NewVBox(
		header,
		widget.NewSeparator(),
		widget.NewLabel("Analyzer URL:"),
		analyzerURLEntry,
		widget.NewLabel("API Key:"),
		apiKeyEntry,
		ignoreTLSCheck,
		//widget.NewSeparator(),
	)

	scrollContainer := container.NewScroll(container.NewPadded(container.NewVBox(
		layout.NewSpacer(),
		content,
		layout.NewSpacer(),
	)))
	w.window.SetContent(container.NewPadded(container.NewBorder(nil, buttonBar, nil, nil, scrollContainer)))
}

func (w *WizardApp) showTimeIntervalScreen() {
	w.currentScreen = 2
	w.ensureDefaultDates()
	log.Printf("screen: time_interval start=%s end=%s", w.startDate, w.endDate)

	header := w.stepHeader(2,
		"Time Interval Selection",
		"Select the time interval for the submission data you want to download.")

	startDateEntry := widget.NewDateEntry()
	if t, err := time.Parse("2006-01-02", w.startDate); err == nil {
		startDateEntry.SetDate(&t)
	}
	startDateEntry.OnChanged = func(t *time.Time) {
		if t == nil {
			w.startDate = ""
			return
		}
		w.startDate = t.Format("2006-01-02")
	}

	endDateEntry := widget.NewDateEntry()
	if t, err := time.Parse("2006-01-02", w.endDate); err == nil {
		endDateEntry.SetDate(&t)
	}
	endDateEntry.OnChanged = func(t *time.Time) {
		if t == nil {
			w.endDate = ""
			return
		}
		w.endDate = t.Format("2006-01-02")
	}

	continueBtn := widget.NewButton("Continue", func() {
		if strings.TrimSpace(w.startDate) != "" && strings.TrimSpace(w.endDate) != "" {
			w.showOutputFolderScreen()
		}
	})
	continueBtn.Importance = widget.HighImportance

	backBtn := widget.NewButton("Back", func() {
		w.showAPIKeyScreen()
	})
	buttonBar := container.NewVBox(
		widget.NewSeparator(),
		container.NewPadded(container.NewHBox(backBtn, layout.NewSpacer(), continueBtn)),
	)

	content := container.NewVBox(
		header,
		widget.NewSeparator(),
		layout.NewSpacer(),
		container.NewVBox(
			widget.NewLabel("Start Date:"),
			startDateEntry,
			widget.NewLabel("End Date:"),
			endDateEntry,
		),
		layout.NewSpacer(),
		//widget.NewSeparator(),
	)

	scrollContainer := container.NewScroll(container.NewPadded(
		//container.NewVBox(
		//	layout.NewSpacer(),
		content,
		//	layout.NewSpacer(),
		//)
	))
	w.window.SetContent(container.NewPadded(container.NewBorder(nil, buttonBar, nil, nil, scrollContainer)))
}

func (w *WizardApp) showOutputFolderScreen() {
	w.currentScreen = 3
	log.Printf("screen: output_folder")

	header := w.stepHeader(3,
		"Output Folder Selection",
		"Choose the folder where you want to save the CSV file.")

	folderLabel := widget.NewLabel("No folder selected")
	if strings.TrimSpace(w.outputDir) == "" {
		w.outputDir = defaultDownloadsDir()
		if w.outputDir != "" {
			w.saveOutputDir(w.outputDir)
		}
	}
	if w.outputDir != "" {
		folderLabel.SetText(w.outputDir)
	}

	fileNameEntry := widget.NewEntry()
	if strings.TrimSpace(w.outputName) == "" {
		w.outputName = w.defaultOutputName()
	}
	fileNameEntry.SetText(w.outputName)

	selectBtn := widget.NewButton("Select Folder", func() {
		d := dialog.NewFolderOpen(func(uri fyne.ListableURI, err error) {
			if err != nil {
				folderLabel.SetText("Error selecting folder: " + err.Error())
				return
			}
			if uri == nil {
				return
			}
			w.outputDir = uri.Path()
			folderLabel.SetText(w.outputDir)
			w.saveOutputDir(w.outputDir)
		}, w.window)
		d.Show()
	})

	continueBtn := widget.NewButton("Continue", func() {
		if strings.TrimSpace(w.outputDir) == "" {
			folderLabel.SetText("Please select a folder")
			log.Printf("output_folder: missing outputDir")
			return
		}
		name := strings.TrimSpace(fileNameEntry.Text)
		if name == "" {
			name = w.defaultOutputName()
		}
		w.outputName = name
		w.outputPath = filepath.Join(w.outputDir, w.outputName)
		log.Printf("output configured: dir=%q name=%q path=%q", w.outputDir, w.outputName, w.outputPath)
		w.showDownloadScreen()
	})
	continueBtn.Importance = widget.HighImportance

	backBtn := widget.NewButton("Back", func() {
		w.showTimeIntervalScreen()
	})
	buttonBar := container.NewVBox(
		widget.NewSeparator(),
		container.NewPadded(container.NewHBox(backBtn, layout.NewSpacer(), continueBtn)),
	)

	content := container.NewVBox(
		header,
		widget.NewSeparator(),
		layout.NewSpacer(),
		folderLabel,
		selectBtn,
		widget.NewLabel("Output file name:"),
		fileNameEntry,
		layout.NewSpacer(),
	)

	scrollContainer := container.NewScroll(container.NewPadded(content))
	w.window.SetContent(container.NewPadded(container.NewBorder(nil, buttonBar, nil, nil, scrollContainer)))
}

func (w *WizardApp) showDownloadScreen() {
	w.currentScreen = 4
	log.Printf("screen: download")

	header := w.stepHeader(4,
		"Downloading Data",
		"Downloading submission data from Trend Micro Deep Discovery Analyzer...")

	progressData := binding.NewFloat()
	_ = progressData.Set(0.0)
	progressBar := widget.NewProgressBarWithData(progressData)

	statusData := binding.NewString()
	_ = statusData.Set("Initializing...")
	statusLabel := widget.NewLabelWithData(statusData)

	backBtn := widget.NewButton("Back", func() {
		w.showOutputFolderScreen()
	})
	buttonBar := container.NewVBox(
		widget.NewSeparator(),
		container.NewPadded(container.NewHBox(backBtn)),
	)
	form := container.NewVBox(
		widget.NewLabel("Progress:"),
		progressBar,
		widget.NewLabel("Status:"),
		statusLabel,
	)

	centered := container.New(
		layout.NewVBoxLayout(),
		layout.NewSpacer(),
		form,
		layout.NewSpacer(),
	)

	content := container.NewBorder(
		container.NewVBox(
			header,
			widget.NewSeparator(),
		),
		nil, nil, nil,
		container.New(layout.NewStackLayout(), centered),
	)
	scrollContainer := container.NewScroll(container.NewPadded(content))
	w.window.SetContent(container.NewPadded(container.NewBorder(nil, buttonBar, nil, nil, scrollContainer)))

	// Integrate with DDAn API and generate CSV
	backBtn.Disable()
	go w.downloadAndGenerateCSV(progressData, statusData, backBtn)
}

func (w *WizardApp) downloadAndGenerateCSV(progressData binding.Float, statusData binding.String, backBtn *widget.Button) {
	ctx := context.Background()
	if w.verbose {
		ctx = ddan.VerboseContext(ctx, func(line string) {
			log.Printf("ddan: %s", strings.TrimSpace(line))
		})
	}
	setProgress := func(v float64) { _ = progressData.Set(v) }
	setStatus := func(s string) { _ = statusData.Set(s) }

	if err := w.runExport(ctx, setProgress, setStatus); err != nil {
		setStatus("Error: " + err.Error())
		log.Printf("error: export: %v", err)
		if backBtn != nil {
			fyne.Do(func() {
				backBtn.Enable()
			})
		}
		return
	}

	setStatus("Download complete!")
	setProgress(progressComplete)
	log.Printf("download complete")

	// Show completion screen after a short delay
	time.Sleep(1 * time.Second)
	fyne.Do(func() {
		w.showCompletionScreen()
	})
}

func (w *WizardApp) runExport(ctx context.Context, setProgress func(float64), setStatus func(string)) error {
	log.Printf("download start: analyzer_url=%q ignore_tls=%v start=%s end=%s output=%q", w.analyzerURL, w.ignoreTLS, w.startDate, w.endDate, w.outputPath)

	setStatus("Connecting to DDAn API...")
	setProgress(progressConnect)

	analyzerURL := strings.TrimSpace(w.analyzerURL)
	if !strings.Contains(analyzerURL, "://") {
		analyzerURL = "https://" + analyzerURL
	}
	u, err := url.Parse(analyzerURL)
	if err != nil {
		return fmt.Errorf("invalid analyzer url %q: %w", analyzerURL, err)
	}

	localHost, err := os.Hostname()
	if err != nil || strings.TrimSpace(localHost) == "" {
		localHost = "localhost"
	}

	client := ddan.NewClient("submissions-downloader", localHost)
	client.SetAnalyzer(u, w.apiKey, w.ignoreTLS)
	client.SetSource(sourceID, sourceName)
	client.SetUUID(w.ensureClientUUID())

	w.mu.Lock()
	w.activeClient = client
	w.registered = false
	w.mu.Unlock()

	testCtx, testCancel := context.WithTimeout(ctx, apiCallTimeout)
	if err := client.TestConnection(testCtx); err != nil {
		testCancel()
		return fmt.Errorf("test connection: %w", err)
	}
	testCancel()

	setStatus("Registering...")
	newUUID, oldUUID, err := w.rotateClientUUIDForRegister(w.clientUUID)
	if err != nil {
		return fmt.Errorf("prepare client uuid: %w", err)
	}
	client.SetUUID(newUUID)
	regCtx, regCancel := context.WithTimeout(ctx, apiCallTimeout)
	if err := client.Register(regCtx); err != nil {
		regCancel()
		if errors.Is(err, ddan.ErrAlreadyRegistered) {
			if strings.TrimSpace(oldUUID) == "" {
				return fmt.Errorf("client already registered in Analyzer; please reset registration in Analyzer Web UI (Submitters list) and retry")
			}
			client.SetUUID(oldUUID)
			regCtx2, regCancel2 := context.WithTimeout(ctx, apiCallTimeout)
			err2 := client.Register(regCtx2)
			regCancel2()
			if err2 == nil {
				_ = keyring.Set(keyringService, keyringUUID, oldUUID)
				_ = keyring.Delete(keyringService, keyringUUIDOld)
			} else if errors.Is(err2, ddan.ErrAlreadyRegistered) {
				return fmt.Errorf("client already registered in Analyzer; please reset registration in Analyzer Web UI (Submitters list) and retry")
			} else {
				return fmt.Errorf("register with old uuid: %w", err2)
			}
		} else {
			return fmt.Errorf("register: %w", err)
		}
	}
	regCancel()
	_ = keyring.Delete(keyringService, keyringUUIDOld)
	log.Printf("registered")

	w.mu.Lock()
	w.registered = true
	w.mu.Unlock()
	defer func() {
		log.Printf("deferred unregister")
		w.bestEffortUnregister()
		w.mu.Lock()
		w.activeClient = nil
		w.registered = false
		w.mu.Unlock()
	}()

	setStatus("Fetching submission list...")
	setProgress(progressListStart)

	startTime, err := time.Parse("2006-01-02", w.startDate)
	if err != nil {
		return fmt.Errorf("parse start date: %w", err)
	}

	endTime, err := time.Parse("2006-01-02", w.endDate)
	if err != nil {
		return fmt.Errorf("parse end date: %w", err)
	}

	// Use QuerySampleList to retrieve SRIDs for the interval.
	intervalStart := startTime
	intervalEnd := endTime.Add(24 * time.Hour)
	listCtx, listCancel := context.WithTimeout(ctx, apiCallTimeout)
	sridList, err := client.QuerySampleList(listCtx, intervalStart, intervalEnd, "all")
	listCancel()
	if err != nil {
		return fmt.Errorf("query sample list: %w", err)
	}
	srids := sridList.List.SRID

	log.Printf("srid list size: %d", len(srids))
	if err := os.MkdirAll(filepath.Dir(w.outputPath), 0o755); err != nil {
		return err
	}
	f, err := os.Create(w.outputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	header := buildCSVHeader()
	if err := writer.Write(header); err != nil {
		return err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}

	rateTokens := make(chan struct{}, 1)
	rateTokens <- struct{}{}
	ticker := time.NewTicker(apiCallInterval)
	defer ticker.Stop()
	rateCtx, rateCancel := context.WithCancel(ctx)
	defer rateCancel()
	go func() {
		for {
			select {
			case <-rateCtx.Done():
				return
			case <-ticker.C:
				select {
				case rateTokens <- struct{}{}:
				default:
				}
			}
		}
	}()

	acquireRate := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rateTokens:
			return nil
		}
	}

	type reportJob struct {
		idx  int
		srid string
	}

	workers := getReportGoroutines
	if workers < 1 {
		workers = 1
	}

	jobBuf := len(srids)
	if jobBuf < 1 {
		jobBuf = 1
	}
	jobs := make(chan reportJob, jobBuf)
	completed := make(chan int, len(srids))
	errCh := make(chan error, 1)
	rowsCh := make(chan reportRow, workers)
	var apiSeq uint64
	var lastAPILogNano int64

	var wg sync.WaitGroup
	for wi := 0; wi < workers; wi++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if err := acquireRate(); err != nil {
					select {
					case errCh <- err:
					default:
					}
					rateCancel()
					return
				}

				n := atomic.AddUint64(&apiSeq, 1)
				if apiLogEvery > 0 && n%apiLogEvery == 0 {
					now := time.Now().UnixNano()
					last := atomic.LoadInt64(&lastAPILogNano)
					if now-last >= apiLogMinInterval.Nanoseconds() && atomic.CompareAndSwapInt64(&lastAPILogNano, last, now) {
						log.Printf("api[%d]: SampleInfo srid=%s", n, job.srid)
					}
				}
				siCtx, siCancel := context.WithTimeout(ctx, apiCallTimeout)
				si, err := client.SampleInfo(siCtx, job.srid)
				siCancel()
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						rowsCh <- reportRow{SRID: job.srid, DownloadError: err.Error()}
						completed <- job.idx
						continue
					}
					select {
					case errCh <- fmt.Errorf("sample info srid=%s: %w", job.srid, err):
					default:
					}
					rateCancel()
					return
				}

				if err := acquireRate(); err != nil {
					select {
					case errCh <- err:
					default:
					}
					rateCancel()
					return
				}

				n = atomic.AddUint64(&apiSeq, 1)
				if apiLogEvery > 0 && n%apiLogEvery == 0 {
					now := time.Now().UnixNano()
					last := atomic.LoadInt64(&lastAPILogNano)
					if now-last >= apiLogMinInterval.Nanoseconds() && atomic.CompareAndSwapInt64(&lastAPILogNano, last, now) {
						log.Printf("api[%d]: GetReport sha1=%s srid=%s", n, si.SHA1MessageID, job.srid)
					}
				}
				repCtx, repCancel := context.WithTimeout(ctx, apiCallTimeout)
				rep, err := client.GetReport(repCtx, si.SHA1MessageID)
				repCancel()
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						rowsCh <- reportRow{SRID: job.srid, SampleInfo: si, DownloadError: err.Error()}
						completed <- job.idx
						continue
					}
					select {
					case errCh <- fmt.Errorf("get report sha1=%s: %w", si.SHA1MessageID, err):
					default:
					}
					rateCancel()
					return
				}

				var far *report27.FILEANALYZEREPORT
				farLen := 0
				if rep != nil {
					farLen = len(rep.FILEANALYZEREPORT)
					if farLen > 0 {
						far = rep.FILEANALYZEREPORT[0]
					}
				}

				rowsCh <- reportRow{
					SRID:                 job.srid,
					SampleInfo:           si,
					FileAnalyzeReport:    far,
					FileAnalyzeReportLen: farLen,
				}
				completed <- job.idx
			}
		}()
	}

	var writeWG sync.WaitGroup
	writeWG.Add(1)
	go func() {
		defer writeWG.Done()
		written := 0
		for r := range rowsCh {
			atomic.StoreInt64(&lastAPILogNano, time.Now().UnixNano())
			m := reportRowToMap(r)
			rec := make([]string, 0, len(header))
			for _, k := range header {
				rec = append(rec, m[k])
			}
			if err := writer.Write(rec); err != nil {
				select {
				case errCh <- err:
				default:
				}
				rateCancel()
				return
			}
			written++
			if written%100 == 0 {
				writer.Flush()
				if err := writer.Error(); err != nil {
					select {
					case errCh <- err:
					default:
					}
					rateCancel()
					return
				}
			}
			if written == 1 || written%100 == 0 {
				log.Printf("csv: written rows=%d", written)
			}
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			select {
			case errCh <- err:
			default:
			}
			rateCancel()
			return
		}
		log.Printf("csv: writer done rows=%d", written)
	}()

	queued := 0
	total := len(srids)
	stallTicker := time.NewTicker(30 * time.Second)
	defer stallTicker.Stop()
	var lastProgressNano int64
	atomic.StoreInt64(&lastProgressNano, time.Now().UnixNano())
	for i, srid := range srids {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			close(rowsCh)
			writeWG.Wait()
			return ctx.Err()
		case jobs <- reportJob{idx: i, srid: srid}:
		}
		queued++
		setStatus(fmt.Sprintf("Queued %d/%d...", queued, total))
		if total > 0 {
			setProgress(progressListStart + (progressQueueSpan * (float64(queued) / float64(total))))
		}
	}
	close(jobs)

	doneCount := 0
	lastUIUpdateNano := int64(0)
	uiUpdateMinInterval := 200 * time.Millisecond
	for doneCount < total {
		select {
		case err := <-errCh:
			wg.Wait()
			close(rowsCh)
			writeWG.Wait()
			return err
		case <-ctx.Done():
			wg.Wait()
			close(rowsCh)
			writeWG.Wait()
			return ctx.Err()
		case <-completed:
			doneCount++
			atomic.StoreInt64(&lastProgressNano, time.Now().UnixNano())
			now := time.Now().UnixNano()
			last := atomic.LoadInt64(&lastUIUpdateNano)
			if doneCount == 1 || doneCount == total || now-last >= uiUpdateMinInterval.Nanoseconds() {
				if atomic.CompareAndSwapInt64(&lastUIUpdateNano, last, now) {
					setStatus(fmt.Sprintf("Downloaded %d/%d...", doneCount, total))
					if total > 0 {
						setProgress(progressDoneStart + (progressDoneSpan * (float64(doneCount) / float64(total))))
					}
				}
			}
			if doneCount == 1 || doneCount%100 == 0 {
				log.Printf("progress: downloaded %d/%d", doneCount, total)
			}
		case <-stallTicker.C:
			last := atomic.LoadInt64(&lastProgressNano)
			if time.Since(time.Unix(0, last)) >= stallTimeout {
				wg.Wait()
				close(rowsCh)
				writeWG.Wait()
				return fmt.Errorf("stalled for %s without progress", stallTimeout)
			}
		}
	}
	wg.Wait()
	close(rowsCh)
	writeWG.Wait()

	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}

	setStatus("Finalizing...")
	setProgress(progressFinalizing)
	log.Printf("csv generated: %q rows=%d", w.outputPath, total)
	return nil
}

func (w *WizardApp) showCompletionScreen() {
	w.currentScreen = 5

	header := w.stepHeader(5,
		"Download Complete!",
		"Your CSV file has been generated successfully.")

	fileLink := container.NewVBox(
		widget.NewLabel("Result:"),
		newLinkLabel(w.outputPath, func() {
			if err := openFile(w.outputPath); err != nil {
				dialog.ShowError(err, w.window)
			}
		}),
	)
	finishBtn := widget.NewButton("Finish", func() {
		w.window.Close()
	})

	newDownloadBtn := widget.NewButton("Start New Download", func() {
		w.showIntroScreen()
	})
	buttonBar := container.NewVBox(
		widget.NewSeparator(),
		container.NewPadded(container.NewHBox(newDownloadBtn, layout.NewSpacer(), finishBtn)),
	)

	content := container.NewVBox(
		header,
		widget.NewSeparator(),
		layout.NewSpacer(),
		fileLink,
		layout.NewSpacer(),
	)

	scrollContainer := container.NewScroll(container.NewPadded(content))
	w.window.SetContent(container.NewPadded(container.NewBorder(nil, buttonBar, nil, nil, scrollContainer)))
}
