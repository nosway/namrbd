package structuredlog

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type Field struct {
	Key   string
	Value any
}

var (
	loggerMu sync.RWMutex
	logger   = log.New(os.Stderr, "", 0)
)

func F(key string, value any) Field {
	return Field{Key: key, Value: value}
}

func SetOutput(w io.Writer) func() {
	if w == nil {
		w = io.Discard
	}
	return SetLogger(log.New(w, "", 0))
}

func SetLogger(l *log.Logger) func() {
	if l == nil {
		l = log.New(io.Discard, "", 0)
	}
	loggerMu.Lock()
	prev := logger
	logger = l
	loggerMu.Unlock()
	return func() {
		loggerMu.Lock()
		logger = prev
		loggerMu.Unlock()
	}
}

func Info(component, event string, fields ...Field) {
	emit("info", component, event, nil, fields...)
}

func Error(component, event string, err error, fields ...Field) {
	emit("error", component, event, err, fields...)
}

func emit(level, component, event string, err error, fields ...Field) {
	record := map[string]any{
		"ts":        time.Now().UTC().Format(time.RFC3339Nano),
		"level":     level,
		"component": component,
		"event":     event,
	}
	for _, field := range fields {
		if field.Key == "" {
			continue
		}
		record[field.Key] = field.Value
	}
	if err != nil {
		record["error"] = err.Error()
	}
	raw, marshalErr := json.Marshal(record)
	if marshalErr != nil {
		raw = []byte(`{"level":"error","component":"structuredlog","event":"marshal_failed"}`)
	}
	loggerMu.RLock()
	current := logger
	loggerMu.RUnlock()
	current.Print(string(raw))
}
