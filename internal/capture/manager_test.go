package capture

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/maintainer64/cms-labs-capture/internal/config"
	"github.com/maintainer64/cms-labs-capture/internal/kube"
)

type fakeBackend struct {
	release chan struct{}
}

func (f *fakeBackend) Targets(context.Context) ([]kube.Target, error) {
	return []kube.Target{{Node: "r1", Interfaces: []string{"eth1"}}}, nil
}

func (f *fakeBackend) Capture(ctx context.Context, _ kube.CaptureSpec, output io.Writer) error {
	header := make([]byte, pcapHeaderLength)
	binary.LittleEndian.PutUint32(header, 0xa1b2c3d4)
	if _, err := output.Write(header); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.release:
		return nil
	}
}

func TestCaptureStartStopAndDownload(t *testing.T) {
	t.Parallel()
	backend := &fakeBackend{release: make(chan struct{})}
	configuration := config.Config{Storage: config.Storage{Path: t.TempDir()}}
	manager, err := NewManager(context.Background(), backend, configuration)
	if err != nil {
		t.Fatal(err)
	}
	record, err := manager.Start(context.Background(), StartRequest{Node: "r1", Interface: "eth1"})
	if err != nil || record.State != "capturing" {
		t.Fatalf("Start = %#v, %v", record, err)
	}
	if _, err = manager.Stop(record.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		record, err = manager.Get(record.ID)
		if err == nil && record.State == "stopped" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	file, _, err := manager.Open(record.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			t.Errorf("closing PCAP: %v", closeErr)
		}
	}()
	raw, err := io.ReadAll(file)
	if err != nil || len(raw) != pcapHeaderLength {
		t.Fatalf("pcap length = %d, error %v", len(raw), err)
	}
}

func TestCaptureConcurrencyLimit(t *testing.T) {
	t.Parallel()
	backend := &fakeBackend{release: make(chan struct{})}
	configuration := config.Config{
		Limits: config.Limits{MaxConcurrent: 1}, Storage: config.Storage{Path: t.TempDir()},
	}
	manager, err := NewManager(context.Background(), backend, configuration)
	if err != nil {
		t.Fatal(err)
	}
	first, err := manager.Start(context.Background(), StartRequest{Node: "r1", Interface: "eth1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = manager.Start(context.Background(), StartRequest{Node: "r1", Interface: "eth1"}); !errors.Is(err, ErrBusy) {
		t.Fatalf("second capture error = %v", err)
	}
	_, _ = manager.Stop(first.ID)
}

func TestCaptureRejectsInvalidLimitsBeforeStartingBackend(t *testing.T) {
	t.Parallel()
	backend := &fakeBackend{release: make(chan struct{})}
	manager, err := NewManager(context.Background(), backend, config.Config{
		Storage: config.Storage{Path: t.TempDir()},
	})
	if err != nil {
		t.Fatal(err)
	}
	requests := []StartRequest{
		{Node: "r1", Interface: "eth1", DurationSeconds: -1},
		{Node: "r1", Interface: "eth1", MaxBytes: -1},
		{Node: "r1", Interface: "eth 1"},
	}
	for _, request := range requests {
		if _, startErr := manager.Start(context.Background(), request); !errors.Is(startErr, ErrInvalid) {
			t.Fatalf("Start(%#v) error = %v", request, startErr)
		}
	}
}
