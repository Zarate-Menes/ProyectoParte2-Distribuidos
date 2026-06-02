package logger

import (
	"context"
	"io"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type loggerKey string

const loggerContextKey loggerKey = "ctx_logger"

func newEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

func newLogger(debugMode, prettyLog bool) *zap.SugaredLogger {
	cfg := newEncoderConfig()
	if prettyLog {
		cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		cfg.EncodeLevel = zapcore.CapitalLevelEncoder
	}
	logLevel := zapcore.InfoLevel
	if debugMode {
		logLevel = zapcore.DebugLevel
	}
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(cfg),
		zapcore.Lock(os.Stdout),
		logLevel,
	)
	return zap.New(core, zap.AddCaller()).Sugar()
}

// NewLoggerToWriter writes to w with ANSI colors.
func NewLoggerToWriter(w io.Writer, debugMode bool) *zap.SugaredLogger {
	cfg := newEncoderConfig()
	cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logLevel := zapcore.InfoLevel
	if debugMode {
		logLevel = zapcore.DebugLevel
	}
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(cfg),
		zapcore.AddSync(w),
		logLevel,
	)
	return zap.New(core, zap.AddCaller()).Sugar()
}

func GetContextLogger(ctx context.Context) *zap.SugaredLogger {
	if ctx == nil {
		ctx = context.Background()
	}
	log, ok := ctx.Value(loggerContextKey).(*zap.SugaredLogger)
	if !ok {
		log := newLogger(true, true)
		log.Warn("logger not found in context, creating a new one")
		return log
	}
	return log
}

func SetContextLogger(ctx context.Context, log *zap.SugaredLogger) context.Context {
	return context.WithValue(ctx, loggerContextKey, log)
}

func NewLogger(params ...bool) *zap.SugaredLogger {
	var prettyLog, debugMode bool
	if len(params) == 0 {
		prettyLog = true
		debugMode = true
	} else if len(params) == 1 {
		prettyLog = params[0]
		debugMode = params[0]
	} else {
		debugMode = params[0]
		prettyLog = params[1]
	}
	return newLogger(debugMode, prettyLog)
}
