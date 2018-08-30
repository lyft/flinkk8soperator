package logger

import (
	"context"
	"github.com/sirupsen/logrus"
	"fmt"
	"runtime"
	"strings"
)

type Config struct {
	IncludeSourceCode bool
}

var config = Config{}

func SetConfig(cfg Config) {
	config = cfg
}
func getSourceCode() string {
	if config.IncludeSourceCode {
		_, file, line, ok := runtime.Caller(3)
		if !ok {
			file = "???"
			line = 1
		} else {
			slash := strings.LastIndex(file, "/")
			if slash >= 0 {
				file = file[slash+1:]
			}
		}
		return fmt.Sprintf("%v:%v", file, line)
	}
	return ""
}
func wrapHeader(args ...interface{}) []interface{} {
	if config.IncludeSourceCode {
		return append(
			[]interface{}{
				fmt.Sprintf("[%v] ", getSourceCode()),
			},
			args...)
	}
	return args
}
func wrapHeaderForMessage(message string) string {
	if config.IncludeSourceCode {
		return fmt.Sprintf("[%v] %v", getSourceCode(), message)
	}
	return message
}

// Debug logs a message at level Debug on the standard logger.
func Debug(ctx context.Context, args ...interface{}) {
	logrus.Debug(wrapHeader(args)...)
}

// Print logs a message at level Info on the standard logger.
func Print(ctx context.Context, args ...interface{}) {
	logrus.Print(wrapHeader(args)...)
}

// Info logs a message at level Info on the standard logger.
func Info(ctx context.Context, args ...interface{}) {
	logrus.Info(wrapHeader(args)...)
}

// Warn logs a message at level Warn on the standard logger.
func Warn(ctx context.Context, args ...interface{}) {
	logrus.Warn(wrapHeader(args)...)
}

// Warning logs a message at level Warn on the standard logger.
func Warning(ctx context.Context, args ...interface{}) {
	logrus.Warning(wrapHeader(args)...)
}

// Error logs a message at level Error on the standard logger.
func Error(ctx context.Context, args ...interface{}) {
	logrus.Error(wrapHeader(args)...)
}

// Panic logs a message at level Panic on the standard logger.
func Panic(ctx context.Context, args ...interface{}) {
	logrus.Panic(wrapHeader(args)...)
}

// Fatal logs a message at level Fatal on the standard logger.
func Fatal(ctx context.Context, args ...interface{}) {
	logrus.Fatal(wrapHeader(args)...)
}

// Debugf logs a message at level Debug on the standard logger.
func Debugf(ctx context.Context, format string, args ...interface{}) {
	logrus.Debugf(wrapHeaderForMessage(format), args...)
}

// Printf logs a message at level Info on the standard logger.
func Printf(ctx context.Context, format string, args ...interface{}) {
	logrus.Printf(wrapHeaderForMessage(format), args...)
}

// Infof logs a message at level Info on the standard logger.
func Infof(ctx context.Context, format string, args ...interface{}) {
	logrus.Infof(wrapHeaderForMessage(format), args...)
}

// Warnf logs a message at level Warn on the standard logger.
func Warnf(ctx context.Context, format string, args ...interface{}) {
	logrus.Warnf(wrapHeaderForMessage(format), args...)
}

// Warningf logs a message at level Warn on the standard logger.
func Warningf(ctx context.Context, format string, args ...interface{}) {
	logrus.Warningf(wrapHeaderForMessage(format), args...)
}

// Errorf logs a message at level Error on the standard logger.
func Errorf(ctx context.Context, format string, args ...interface{}) {
	logrus.Errorf(wrapHeaderForMessage(format), args...)
}

// Panicf logs a message at level Panic on the standard logger.
func Panicf(ctx context.Context, format string, args ...interface{}) {
	logrus.Panicf(wrapHeaderForMessage(format), args...)
}

// Fatalf logs a message at level Fatal on the standard logger.
func Fatalf(ctx context.Context, format string, args ...interface{}) {
	logrus.Fatalf(wrapHeaderForMessage(format), args...)
}

// Debugln logs a message at level Debug on the standard logger.
func Debugln(ctx context.Context, args ...interface{}) {
	logrus.Debugln(wrapHeader(args)...)
}

// Println logs a message at level Info on the standard logger.
func Println(ctx context.Context, args ...interface{}) {
	logrus.Println(wrapHeader(args)...)
}

// Infoln logs a message at level Info on the standard logger.
func Infoln(ctx context.Context, args ...interface{}) {
	logrus.Infoln(wrapHeader(args)...)
}

// Warnln logs a message at level Warn on the standard logger.
func Warnln(ctx context.Context, args ...interface{}) {
	logrus.Warnln(wrapHeader(args)...)
}

// Warningln logs a message at level Warn on the standard logger.
func Warningln(ctx context.Context, args ...interface{}) {
	logrus.Warningln(wrapHeader(args)...)
}

// Errorln logs a message at level Error on the standard logger.
func Errorln(ctx context.Context, args ...interface{}) {
	logrus.Errorln(wrapHeader(args)...)
}

// Panicln logs a message at level Panic on the standard logger.
func Panicln(ctx context.Context, args ...interface{}) {
	logrus.Panicln(wrapHeader(args)...)
}

// Fatalln logs a message at level Fatal on the standard logger.
func Fatalln(ctx context.Context, args ...interface{}) {
	logrus.Fatalln(wrapHeader(args)...)
}

func InfofNoCtx(format string, args ...interface{}) {
	Infof(nil, fmt.Sprintf(format, args...))
}