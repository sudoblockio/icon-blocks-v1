package logging

import (
	"os"
	"testing"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-blocks/config"
)

func TestInit(t *testing.T) {
	os.Setenv("LOG_LEVEL", "Info")
  config.ReadEnvironment()

  Init()

  zap.S().Info("Test Log")
}
