package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// ANSI color codes
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorGray   = "\033[37m"
	colorWhite  = "\033[97m"
)

type customHandler struct {
	output   io.Writer
	level    slog.Level
	colorize bool
}

func (h *customHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *customHandler) Handle(ctx context.Context, r slog.Record) error {
	file, line, fn := "unknown", 0, "unknown"

	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "source_file":
			file = a.Value.String()
		case "source_line":
			line = int(a.Value.Int64())
		case "source_func":
			fn = a.Value.String()
		}
		return true
	})

	// Format: <timestamp> <level> <file> <line> <msg>
	timestamp := r.Time.Format("2006-01-02 15:04:05.000")
	level := strings.ToUpper(r.Level.String())

	var logLine string
	if h.colorize {
		var levelColor string
		switch r.Level {
		case slog.LevelDebug:
			levelColor = colorGray
		case slog.LevelInfo:
			levelColor = colorBlue
		case slog.LevelWarn:
			levelColor = colorYellow
		case slog.LevelError:
			levelColor = colorRed
		default:
			levelColor = colorWhite
		}

		logLine = fmt.Sprintf("%s%s%s %s%s%s %s%s:%d:%s%s %s\n",
			colorGray, timestamp, colorReset,
			levelColor, level, colorReset,
			colorWhite, file, line, fn, colorReset,
			r.Message)
	} else {
		logLine = fmt.Sprintf("%s %s %s:%d:%s %s\n",
			timestamp, level, file, line, fn, r.Message)
	}

	_, err := h.output.Write([]byte(logLine))
	return err
}

// WithAttrs returns a new Handler whose attributes consist of both the receiver's attributes and the given ones
func (h *customHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h // We don't support attributes for simplicity
}

// WithGroup returns a new Handler with the given group appended to the receiver's existing groups
func (h *customHandler) WithGroup(name string) slog.Handler {
	return h // We don't support groups for simplicity
}

// Logger is the main logger struct
type Logger struct {
	slogger *slog.Logger
	level   slog.Level
}

// New creates a new logger instance
func New(level slog.Level, output io.Writer) *Logger {
	if output == nil {
		output = os.Stdout
	}

	// Check if output is stdout to enable colors
	colorize := output == os.Stdout

	handler := &customHandler{
		output:   output,
		level:    level,
		colorize: colorize,
	}

	return &Logger{
		slogger: slog.New(handler),
		level:   level,
	}
}

// NewDefault creates a logger with default settings (INFO level, stdout)
func NewDefault() *Logger {
	return New(slog.LevelInfo, os.Stdout)
}

// logWithCaller logs a message with caller information
func (l *Logger) logWithCaller(level slog.Level, msg string, args ...any) {
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}

	fun := "unknown"
	fp, file, line, ok := runtime.Caller(2)

	if ok {
		file = filepath.Base(file)
		f := runtime.FuncForPC(fp)
		fun = f.Name()
	} else {
		file, line = "unknown", 0
	}

	l.slogger.LogAttrs(
		context.Background(),
		level,
		msg,
		slog.String("source_file", file),
		slog.Int("source_line", line),
		slog.String("source_func", fun),
	)
}

// Debug logs a debug message
func (l *Logger) Debug(msg string, args ...any) {
	l.logWithCaller(slog.LevelDebug, msg, args...)
}

// Info logs an info message
func (l *Logger) Info(msg string, args ...any) {
	l.logWithCaller(slog.LevelInfo, msg, args...)
}

// Warn logs a warning message
func (l *Logger) Warn(msg string, args ...any) {
	l.logWithCaller(slog.LevelWarn, msg, args...)
}

// Error logs an error message
func (l *Logger) Error(msg string, args ...any) {
	l.logWithCaller(slog.LevelError, msg, args...)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(msg string, args ...any) {
	l.logWithCaller(slog.LevelError, msg, args...)
	os.Exit(1)
}

// SetLevel sets the logging level
func (l *Logger) SetLevel(level slog.Level) {
	l.level = level
	// Recreate the logger with the new level
	var output io.Writer = os.Stdout
	colorize := true

	// Try to determine if we're using stdout by checking the handler
	if handler, ok := l.slogger.Handler().(*customHandler); ok {
		output = handler.output
		colorize = handler.colorize
	}

	newHandler := &customHandler{
		output:   output,
		level:    level,
		colorize: colorize,
	}

	l.slogger = slog.New(newHandler)
}

// GetLevel returns the current logging level
func (l *Logger) GetLevel() slog.Level {
	return l.level
}

// Global logger instance
var defaultLogger = NewDefault()

// Global logging functions

// Debug logs a debug message using the global logger
func Debug(msg string, args ...any) {
	defaultLogger.logWithCaller(slog.LevelDebug, msg, args...)
}

// Info logs an info message using the global logger
func Info(msg string, args ...any) {
	defaultLogger.logWithCaller(slog.LevelInfo, msg, args...)
}

// Warn logs a warning message using the global logger
func Warn(msg string, args ...any) {
	defaultLogger.logWithCaller(slog.LevelWarn, msg, args...)
}

// Error logs an error message using the global logger
func Error(msg string, args ...any) {
	defaultLogger.logWithCaller(slog.LevelError, msg, args...)
}

// Fatal logs a fatal message using the global logger and exits
func Fatal(msg string, args ...any) {
	defaultLogger.logWithCaller(slog.LevelError, msg, args...)
	os.Exit(1)
}

// SetLevel sets the logging level on the global logger
func SetLevel(level slog.Level) {
	defaultLogger.SetLevel(level)
}

// GetLevel returns the current logging level from the global logger
func GetLevel() slog.Level {
	return defaultLogger.GetLevel()
}

// SetGlobalLogger sets a custom global logger
func SetGlobalLogger(l *Logger) {
	defaultLogger = l
}

// GetGlobalLogger returns the current global logger
func GetGlobalLogger() *Logger {
	return defaultLogger
}
