package logging

import (
	"log"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/global"
)

// Init - init logging config
func Init() {
	cfg := newLoggerConfig()

	logger := newLogger(cfg)
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	<-global.ShutdownChan
}

func newLogger(cfg zap.Config) *zap.Logger {
	logger, err := cfg.Build()
	if err != nil {
		log.Fatal("Cannot Initialize logger")
	}
	return logger
}

func newLoggerConfig() zap.Config {
	cfg := zap.Config{
		Level:            setLoggerConfigLogLevel(),
		Development:      true,
		Encoding:         "console",
		EncoderConfig:    newLoggerEncoderConfig(),
		OutputPaths:      setLoggerConfigOutputPaths(),
		ErrorOutputPaths: setLoggerConfigErrorOutputPaths(),
	}
	return cfg
}

func newLoggerEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,   //zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,    //zapcore.EpochTimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder, //zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

func setLoggerConfigLogLevel() zap.AtomicLevel {
	var atomicLevel zap.AtomicLevel

	switch strings.ToUpper(config.Config.LogLevel) {
	case "PANIC":
		atomicLevel = zap.NewAtomicLevelAt(zap.PanicLevel)
		break
	case "FATAL":
		atomicLevel = zap.NewAtomicLevelAt(zap.FatalLevel)
		break
	case "ERROR":
		atomicLevel = zap.NewAtomicLevelAt(zap.ErrorLevel)
		break
	case "WARN":
		atomicLevel = zap.NewAtomicLevelAt(zap.WarnLevel)
		break
	case "INFO":
		atomicLevel = zap.NewAtomicLevelAt(zap.InfoLevel)
		break
	case "DEBUG":
		atomicLevel = zap.NewAtomicLevelAt(zap.DebugLevel)
		break
	default:
		atomicLevel = zap.NewAtomicLevelAt(zap.DebugLevel)
	}
	return atomicLevel
}

func setLoggerConfigOutputPaths() []string {
	outputPaths := []string{"stderr"}
	if config.Config.LogToFile == true {
		outputPaths = append(outputPaths, config.Config.LogFileName)
	}
	return outputPaths
}

func setLoggerConfigErrorOutputPaths() []string {
	errorOutputPaths := []string{"stderr"}
	if config.Config.LogToFile == true {
		errorOutputPaths = append(errorOutputPaths, config.Config.LogFileName)
	}
	return errorOutputPaths
}
