package main

import (
	"context"
	"embed"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
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
	prefAnalyzerURL = "analyzer_url"
	prefClientUUID  = "client_uuid"
	prefIgnoreTLS   = "ignore_tls"
	prefOutputDir   = "output_dir"

	apiCallInterval     = 1 * time.Millisecond
	getReportGoroutines = 60

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
	img.SetMinSize(fyne.NewSize(280, 160))

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
		w.clientUUID = uuid.NewString()
		return w.clientUUID
	}
	u := strings.TrimSpace(w.app.Preferences().String(prefClientUUID))
	if u != "" {
		return u
	}
	newID := uuid.NewString()
	w.app.Preferences().SetString(prefClientUUID, newID)
	return newID
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
		w.ensureDefaultDates()
		w.initLogging()

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
		wizardApp.window.Close()
	})

	wizardApp.showIntroScreen()

	wizardApp.window.ShowAndRun()
}

func (w *WizardApp) initLogging() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("logging started")
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
	_ = client.Unregister(ctx)
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
	go w.downloadAndGenerateCSV(progressData, statusData)
}

func (w *WizardApp) downloadAndGenerateCSV(progressData binding.Float, statusData binding.String) {
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
		return
	}

	setStatus("Download complete!")
	setProgress(1.0)
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
	setProgress(0.1)

	analyzerURL := strings.TrimSpace(w.analyzerURL)
	if !strings.Contains(analyzerURL, "://") {
		analyzerURL = "https://" + analyzerURL
	}
	u, err := url.Parse(analyzerURL)
	if err != nil {
		return fmt.Errorf("invalid analyzer url %q: %w", analyzerURL, err)
	}

	client := ddan.NewClient("submissions-downloader", u.Hostname())
	client.SetAnalyzer(u, w.apiKey, w.ignoreTLS)
	client.SetSource(sourceID, sourceName)
	client.SetUUID(w.ensureClientUUID())

	w.mu.Lock()
	w.activeClient = client
	w.registered = false
	w.mu.Unlock()

	if err := client.TestConnection(ctx); err != nil {
		return fmt.Errorf("test connection: %w", err)
	}

	setStatus("Registering...")
	if err := client.Register(ctx); err != nil {
		return fmt.Errorf("register: %w", err)
	}
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
	setProgress(0.3)

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
	sridList, err := client.QuerySampleList(ctx, intervalStart, intervalEnd, "all")
	if err != nil {
		return fmt.Errorf("query sample list: %w", err)
	}
	srids := sridList.List.SRID

	log.Printf("srid list size: %d", len(srids))
	results := make([]reportRow, len(srids))

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

	jobs := make(chan reportJob)
	completed := make(chan int, len(srids))
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	workers := getReportGoroutines
	if workers < 1 {
		workers = 1
	}
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

				si, err := client.SampleInfo(ctx, job.srid)
				if err != nil {
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

				rep, err := client.GetReport(ctx, si.SHA1MessageID)
				if err != nil {
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

				results[job.idx].SRID = job.srid
				results[job.idx].SampleInfo = si
				results[job.idx].FileAnalyzeReport = far
				results[job.idx].FileAnalyzeReportLen = farLen

				completed <- job.idx
			}
		}()
	}

	for i, srid := range srids {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		case jobs <- reportJob{idx: i, srid: srid}:
		}
	}
	close(jobs)

	doneCount := 0
	total := len(srids)
	for doneCount < total {
		select {
		case err := <-errCh:
			wg.Wait()
			return err
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case <-completed:
			doneCount++
			setStatus(fmt.Sprintf("Downloaded %d/%d...", doneCount, total))
			if total > 0 {
				setProgress(0.3 + (0.6 * (float64(doneCount) / float64(total))))
			}
		}
	}
	wg.Wait()

	setStatus(fmt.Sprintf("Generating CSV (%d rows)...", len(results)))
	setProgress(0.95)

	if err := w.generateCSV(results); err != nil {
		return fmt.Errorf("generate csv: %w", err)
	}
	log.Printf("csv generated: %q rows=%d", w.outputPath, len(results))
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
