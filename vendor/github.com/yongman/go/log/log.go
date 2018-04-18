//
// log.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package log

import (
	"os"

	"github.com/sirupsen/logrus"
)

var l *logrus.Logger

type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
	FATAL
	PANIC
)

func init() {
	l = logrus.New()
}

func SetLevel(level Level) {
	switch level {
	case DEBUG:
		l.SetLevel(logrus.DebugLevel)
		break
	case INFO:
		l.SetLevel(logrus.InfoLevel)
		break
	case WARN:
		l.SetLevel(logrus.WarnLevel)
		break
	case ERROR:
		l.SetLevel(logrus.ErrorLevel)
		break
	case FATAL:
		l.SetLevel(logrus.FatalLevel)
		break
	case PANIC:
		l.SetLevel(logrus.PanicLevel)
		break
	default:
		break
	}
}

func SetOutput(file *os.File) {
	l.Out = file
}

func SetJsonFormat() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
}

func Debug(i ...interface{}) {
	l.Debug(i...)
}

func Info(i ...interface{}) {
	l.Info(i...)
}

func Warn(i ...interface{}) {
	l.Warn(i...)
}

func Error(i ...interface{}) {
	l.Error(i...)
}

func Fatal(i ...interface{}) {
	l.Fatal(i...)
}

func Panic(i ...interface{}) {
	l.Panic(i...)
}

func Debugf(f string, i ...interface{}) {
	l.Debugf(f, i...)
}

func Infof(f string, i ...interface{}) {
	l.Infof(f, i...)
}

func Warnf(f string, i ...interface{}) {
	l.Warnf(f, i...)
}

func Errorf(f string, i ...interface{}) {
	l.Errorf(f, i...)
}

func Fatalf(f string, i ...interface{}) {
	l.Fatalf(f, i...)
}

func Panicf(f string, i ...interface{}) {
	l.Panicf(f, i...)
}
