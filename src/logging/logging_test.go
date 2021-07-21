//+build unit

package logging

import (
	"os"
	"testing"

	"github.com/geometry-labs/icon-blocks/config"

	log "github.com/sirupsen/logrus"
)

func TestInit(t *testing.T) {

	// Test file logging
	os.Setenv("LOG_LEVEL", "Info")
	os.Setenv("LOG_TO_FILE", "true")
	config.ReadEnvironment()
	StartLoggingInit()
	log.Info("File log")

	// Test levels
	os.Setenv("LOG_LEVEL", "Panic")
	config.ReadEnvironment()
	StartLoggingInit()
	log.Info("Should not log")

	os.Setenv("LOG_LEVEL", "FATAL")
	config.ReadEnvironment()
	StartLoggingInit()
	log.Info("Should not log")

	os.Setenv("LOG_LEVEL", "ERROR")
	config.ReadEnvironment()
	StartLoggingInit()
	log.Info("Should not log")

	os.Setenv("LOG_LEVEL", "WARN")
	config.ReadEnvironment()
	StartLoggingInit()
	log.Warn("Warning")

	os.Setenv("LOG_LEVEL", "INFO")
	config.ReadEnvironment()
	StartLoggingInit()
	log.Info("Info")

	os.Setenv("LOG_LEVEL", "DEBUG")
	config.ReadEnvironment()
	StartLoggingInit()
	log.Debug("Debug")

	os.Setenv("LOG_LEVEL", "TRACE")
	config.ReadEnvironment()
	StartLoggingInit()
	log.Trace("Trace")
}
