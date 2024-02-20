package log

import (
	"io"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var Logger = logrus.New()
var logFileHandle *os.File

func Initialize(file string, verbose bool) (err error) {
	formater := new(prefixed.TextFormatter)
	formater.TimestampFormat = "2006-01-02 15:04:05"
	formater.FullTimestamp = true

	Logger.Formatter = formater

	if len(file) > 0 {
		logFileHandle, err = os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return err
		}

		Logger.Out = io.MultiWriter(logFileHandle, os.Stderr)
	}

	if verbose {
		setLogLevel("debug")
	} else {
		setLogLevel("info")
	}

	return nil
}

func setLogLevel(logLevelString string) {
	switch strings.ToLower(logLevelString) {
	case "debug":
		Logger.SetLevel(logrus.DebugLevel)
	case "info":
		Logger.SetLevel(logrus.InfoLevel)
	case "warning":
		Logger.SetLevel(logrus.WarnLevel)
	case "error":
		Logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		Logger.SetLevel(logrus.FatalLevel)
	default:
		Logger.Warn("Unknown log level \"" + logLevelString + "\". Falling back to INFO.")
	}
}

func ShutDown() {
	logFileHandle.Close()
}
