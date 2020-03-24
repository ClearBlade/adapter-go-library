package adapter_library

import (
	"log"
	"os"
	"strings"

	"github.com/hashicorp/logutils"
)

func loggingInit() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func setLoggingLevel(logLevel string) {
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "INFO", "ERROR", "FATAL"},
		MinLevel: logutils.LogLevel(strings.ToUpper(logLevel)),
		Writer:   os.Stdout,
	}
	log.SetOutput(filter)
}
