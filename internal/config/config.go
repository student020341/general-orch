package config

import (
	"os"
	"strconv"
	"strings"
)

// Config holds shared service connection settings loaded from environment variables.
type Config struct {
	PostgresDSN  string
	NatsURL      string
	TemporalAddr string
	HTTPAddr     string
	ProfilesPath string
	WebDir       string
	TaskQueues   []string
}

// Load reads Config from environment variables with local Docker Compose defaults.
func Load() Config {
	return Config{
		PostgresDSN:  envOr("ORCH_POSTGRES_DSN", "postgres://temporal:temporal@localhost:5432/orch?sslmode=disable"),
		NatsURL:      envOr("ORCH_NATS_URL", "nats://localhost:4222"),
		TemporalAddr: envOr("ORCH_TEMPORAL_ADDR", "localhost:7233"),
		HTTPAddr:     envOr("ORCH_HTTP_ADDR", ":8090"),
		ProfilesPath: envOr("ORCH_PROFILES_PATH", "configs/profiles.json"),
		WebDir:       envOr("ORCH_WEB_DIR", "web"),
		TaskQueues:   splitCSV(envOr("ORCH_TASK_QUEUES", "lighting-workers,motion-workers,pi-workers")),
	}
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// EnvInt parses an integer env var with a default fallback.
func EnvInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
