package log

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	klog "k8s.io/klog/v2"
)

var (
	log  Logger
	atom zap.AtomicLevel
)

type Fields map[string]interface{}

type Logger struct {
	*zap.SugaredLogger
}

func (l Logger) Print(v ...interface{}) {
	l.WithOptions(zap.AddCallerSkip(1)).Info(v...)
}

func (l Logger) Printf(format string, v ...interface{}) {
	l.WithOptions(zap.AddCallerSkip(1)).Infof(format, v...)
}

func (l Logger) Println(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	l.WithOptions(zap.AddCallerSkip(1)).Info(msg[:len(msg)-1])
}

func (l Logger) Warning(v ...interface{}) {
	l.WithOptions(zap.AddCallerSkip(1)).Warn(v...)
}

func (l Logger) Warningf(format string, v ...interface{}) {
	l.WithOptions(zap.AddCallerSkip(1)).Warnf(format, v...)
}

func stringifyLargeFields(v []interface{}) {
	for i, field := range v {
		switch val := field.(type) {
		case int64:
			v[i] = strconv.FormatInt(val, 10)
		case uint64:
			v[i] = strconv.FormatUint(val, 10)
		}
	}
}

func (l Logger) With(v ...interface{}) Logger {
	if len(v) == 0 {
		return Logger{SugaredLogger: l.SugaredLogger}
	}
	stringifyLargeFields(v)
	return Logger{SugaredLogger: l.SugaredLogger.With(v...)}
}

func (l Logger) WithOptions(v ...zap.Option) Logger {
	if len(v) == 0 {
		return Logger{SugaredLogger: l.SugaredLogger}
	}
	return Logger{SugaredLogger: l.SugaredLogger.Desugar().WithOptions(v...).Sugar()}
}

func Info(v ...interface{}) {
	log.WithOptions(zap.AddCallerSkip(2)).Info(v...)
}

func Infof(format string, v ...interface{}) {
	log.WithOptions(zap.AddCallerSkip(2)).Infof(format, v...)
}

func Warn(v ...interface{}) {
	log.WithOptions(zap.AddCallerSkip(2)).Warn(v...)
}

func Warnf(format string, v ...interface{}) {
	log.WithOptions(zap.AddCallerSkip(2)).Warnf(format, v...)
}

func Error(v ...interface{}) {
	log.WithOptions(zap.AddCallerSkip(2)).Error(v...)
}

func Errorf(format string, v ...interface{}) {
	log.WithOptions(zap.AddCallerSkip(2)).Errorf(format, v...)
}

func Print(v ...interface{}) {
	log.WithOptions(zap.AddCallerSkip(2)).Print(v...)
}

func Printf(format string, v ...interface{}) {
	log.WithOptions(zap.AddCallerSkip(2)).Printf(format, v...)
}

func Println(v ...interface{}) {
	log.WithOptions(zap.AddCallerSkip(2)).Println(v...)
}

func Fatal(v ...interface{}) {
	log.WithOptions(zap.AddCallerSkip(2)).Fatal(v...)
}

func Fatalf(format string, v ...interface{}) {
	log.WithOptions(zap.AddCallerSkip(2)).Fatalf(format, v...)
}

func Fatalln(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	log.WithOptions(zap.AddCallerSkip(2)).Fatal(msg)
}

func Panic(v ...interface{}) {
	log.WithOptions(zap.AddCallerSkip(2)).Panic(v...)
}

func Panicf(format string, v ...interface{}) {
	log.WithOptions(zap.AddCallerSkip(2)).Panicf(format, v...)
}

func Panicln(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	log.WithOptions(zap.AddCallerSkip(2)).Panic(msg[:len(msg)-1])
}

func NewEntry() Logger {
	return log
}

const timeKey = "time"

func EnableProductionLogging(lvl zapcore.Level) {
	atom.SetLevel(lvl)
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	encCfg.TimeKey = timeKey
	cfg := zap.Config{
		Level:            atom,
		Encoding:         "json",
		EncoderConfig:    encCfg,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	newLog, _ := cfg.Build()
	log.SugaredLogger = newLog.Sugar()
}

func NewErrorOnlyLogger() Logger {
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	encCfg.TimeKey = timeKey
	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(zapcore.ErrorLevel),
		Encoding:         "json",
		EncoderConfig:    encCfg,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	l, _ := cfg.Build()
	return Logger{SugaredLogger: l.Sugar()}
}

func NewTestLogger(t zaptest.TestingT) Logger {
	return Logger{SugaredLogger: zaptest.NewLogger(t).Sugar()}
}

func Level() zapcore.Level {
	return atom.Level()
}

func LevelHandler() http.Handler {
	return atom
}

func init() {
	cfg := zap.NewDevelopmentConfig()
	atom = cfg.Level
	l, _ := cfg.Build()
	log = Logger{l.Sugar()}

	fs := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(fs)
	defer klog.Flush()
	_ = fs.Set("logtostderr", "false") // Default is "true".
	_ = fs.Set("skip_headers", "true") // Skip headers with ts, etc added by klog.
	klog.SetOutputBySeverity("INFO", &klogWrapper{fn: log.Info})
	klog.SetOutputBySeverity("WARNING", &klogWrapper{fn: log.Warn})
	klog.SetOutputBySeverity("ERROR", &klogWrapper{fn: log.Error})
	klog.SetOutputBySeverity("FATAL", &klogWrapper{fn: log.Fatal})
}

type klogWrapper struct {
	fn func(...interface{})
}

func (w *klogWrapper) Write(p []byte) (n int, err error) {
	w.fn(strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}
