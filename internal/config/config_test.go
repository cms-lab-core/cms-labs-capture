package config

import (
	"errors"
	"testing"
	"time"
)

func TestValidateDefaults(t *testing.T) {
	t.Parallel()
	configuration := Config{}
	if err := configuration.Validate(); err != nil {
		t.Fatal(err)
	}
	if configuration.Server.Address != DefaultAddress || configuration.Limits.MaxConcurrent != 2 ||
		configuration.Limits.MaxBytes != 50<<20 || configuration.Storage.Retention.Duration != 10*time.Minute {
		t.Fatalf("defaults = %#v", configuration)
	}
}

func TestValidateBounds(t *testing.T) {
	t.Parallel()
	configuration := Config{Limits: Limits{MaxConcurrent: 17}}
	if err := configuration.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("error = %v", err)
	}
}

func TestDecodeRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	if _, err := Decode([]byte("unknown: true\n")); err == nil {
		t.Fatal("unknown field was accepted")
	}
}
