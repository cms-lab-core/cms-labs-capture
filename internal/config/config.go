package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultAddress       = "0.0.0.0:8080"
	DefaultStoragePath   = "/var/lib/cms-labs-capture"
	DefaultMaxConcurrent = 2
	DefaultMaxBytes      = int64(50 << 20)
	DefaultMaxPackets    = 100_000
	DefaultSnapLength    = 256 << 10
	maximumMaxBytes      = int64(512 << 20)
)

var ErrInvalid = errors.New("invalid capture configuration")

type Config struct {
	Server  Server  `yaml:"server" json:"server"`
	Limits  Limits  `yaml:"limits" json:"limits"`
	Storage Storage `yaml:"storage" json:"storage"`
}

type Server struct {
	Address        string `yaml:"address" json:"address"`
	IdentityHeader string `yaml:"identityHeader" json:"identityHeader"`
}

type Limits struct {
	MaxConcurrent int      `yaml:"maxConcurrent" json:"maxConcurrent"`
	MaxDuration   Duration `yaml:"maxDuration" json:"maxDuration"`
	MaxBytes      int64    `yaml:"maxBytes" json:"maxBytes"`
	MaxPackets    int      `yaml:"maxPackets" json:"maxPackets"`
	MaxSnapLength int      `yaml:"maxSnapLength" json:"maxSnapLength"`
}

type Storage struct {
	Path      string   `yaml:"path" json:"path"`
	Retention Duration `yaml:"retention" json:"retention"`
}

type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Value == "" {
		return nil
	}
	value, err := time.ParseDuration(node.Value)
	if err != nil {
		return err
	}
	d.Duration = value
	return nil
}

func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

func Decode(raw []byte) (Config, error) {
	var result Config
	decoder := yaml.NewDecoder(strings.NewReader(string(raw)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&result); err != nil {
		return Config{}, err
	}
	return result, nil
}

func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // operator-supplied path.
	if err != nil {
		return Config{}, fmt.Errorf("reading %s: %w", path, err)
	}
	result, err := Decode(raw)
	if err != nil {
		return Config{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	if err = result.Validate(); err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return result, nil
}

func (c *Config) Validate() error {
	if c.Server.Address == "" {
		c.Server.Address = DefaultAddress
	}
	if c.Server.IdentityHeader == "" {
		c.Server.IdentityHeader = "X-CMS-Identity"
	}
	if c.Limits.MaxConcurrent <= 0 {
		c.Limits.MaxConcurrent = DefaultMaxConcurrent
	}
	if c.Limits.MaxConcurrent > 16 {
		return fmt.Errorf("%w: limits.maxConcurrent cannot exceed 16", ErrInvalid)
	}
	if c.Limits.MaxDuration.Duration <= 0 {
		c.Limits.MaxDuration.Duration = time.Minute
	}
	if c.Limits.MaxDuration.Duration > time.Hour {
		return fmt.Errorf("%w: limits.maxDuration cannot exceed 1h", ErrInvalid)
	}
	if c.Limits.MaxBytes <= 0 {
		c.Limits.MaxBytes = DefaultMaxBytes
	}
	if c.Limits.MaxBytes > maximumMaxBytes {
		return fmt.Errorf("%w: limits.maxBytes cannot exceed %d", ErrInvalid, maximumMaxBytes)
	}
	if c.Limits.MaxPackets <= 0 {
		c.Limits.MaxPackets = DefaultMaxPackets
	}
	if c.Limits.MaxPackets > 1_000_000 {
		return fmt.Errorf("%w: limits.maxPackets cannot exceed 1000000", ErrInvalid)
	}
	if c.Limits.MaxSnapLength <= 0 {
		c.Limits.MaxSnapLength = DefaultSnapLength
	}
	if c.Limits.MaxSnapLength < 64 || c.Limits.MaxSnapLength > 1<<20 {
		return fmt.Errorf("%w: limits.maxSnapLength is outside 64..1048576", ErrInvalid)
	}
	if c.Storage.Path == "" {
		c.Storage.Path = DefaultStoragePath
	}
	if !strings.HasPrefix(c.Storage.Path, "/") {
		return fmt.Errorf("%w: storage.path must be absolute", ErrInvalid)
	}
	if c.Storage.Retention.Duration <= 0 {
		c.Storage.Retention.Duration = 10 * time.Minute
	}
	if c.Storage.Retention.Duration > 24*time.Hour {
		return fmt.Errorf("%w: storage.retention cannot exceed 24h", ErrInvalid)
	}
	return nil
}
