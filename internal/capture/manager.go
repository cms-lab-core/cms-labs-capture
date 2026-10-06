package capture

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/cms-lab-core/cms-labs-capture/internal/config"
	"github.com/cms-lab-core/cms-labs-capture/internal/kube"
)

const pcapHeaderLength = 24

var interfaceNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,64}$`)

var (
	ErrNotFound     = errors.New("capture not found")
	ErrBusy         = errors.New("capture concurrency limit reached")
	ErrStillRunning = errors.New("capture is still running")
	ErrMaxBytes     = errors.New("capture byte limit reached")
	ErrInvalid      = errors.New("invalid capture request")
)

type Backend interface {
	Targets(context.Context) ([]kube.Target, error)
	Capture(context.Context, kube.CaptureSpec, io.Writer) error
}

type StartRequest struct {
	Node            string `json:"node"`
	Interface       string `json:"interface"`
	DurationSeconds int    `json:"durationSeconds"`
	PacketLimit     int    `json:"packetLimit"`
	MaxBytes        int64  `json:"maxBytes"`
	SnapLength      int    `json:"snaplen"`
}

type Record struct {
	ID          string    `json:"id"`
	Node        string    `json:"node"`
	Interface   string    `json:"interface"`
	State       string    `json:"state"`
	Bytes       int64     `json:"bytes"`
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt,omitempty"`
	Error       string    `json:"error,omitempty"`
}

type entry struct {
	record Record
	path   string
	cancel context.CancelFunc
}

type Manager struct {
	backend  Backend
	config   config.Config
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.RWMutex
	captures map[string]*entry
	running  int
}

func NewManager(parent context.Context, backend Backend, configuration config.Config) (*Manager, error) {
	if parent == nil || backend == nil {
		return nil, errors.New("manager context and backend are required")
	}
	if err := configuration.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(configuration.Storage.Path, 0o700); err != nil {
		return nil, fmt.Errorf("creating capture storage: %w", err)
	}
	ctx, cancel := context.WithCancel(parent)
	return &Manager{
		backend: backend, config: configuration, ctx: ctx, cancel: cancel,
		captures: make(map[string]*entry),
	}, nil
}

func (m *Manager) Targets(ctx context.Context) ([]kube.Target, error) { return m.backend.Targets(ctx) }

func (m *Manager) Start(requestContext context.Context, request StartRequest) (Record, error) {
	spec, maxBytes, err := m.normalize(request)
	if err != nil {
		return Record{}, err
	}
	id, err := newID()
	if err != nil {
		return Record{}, err
	}
	path := filepath.Join(m.config.Storage.Path, id+".pcap")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // path is a generated ID below the controller-owned storage root.
	if err != nil {
		return Record{}, fmt.Errorf("creating capture file: %w", err)
	}

	m.mu.Lock()
	if m.running >= m.config.Limits.MaxConcurrent {
		m.mu.Unlock()
		_ = file.Close()
		_ = os.Remove(path)
		return Record{}, ErrBusy
	}
	// Keep request values for auditing while deliberately detaching the long-running
	// capture from the HTTP request. Manager shutdown still cancels every capture.
	captureContext, cancel := context.WithCancel(context.WithoutCancel(requestContext))
	stopManagerCancellation := context.AfterFunc(m.ctx, cancel) //nolint:contextcheck // combines the detached request values with the manager lifetime.
	item := &entry{
		record: Record{
			ID: id, Node: spec.Node, Interface: spec.Interface, State: "starting", StartedAt: time.Now().UTC(),
		},
		path: path, cancel: cancel,
	}
	m.captures[id] = item
	m.running++
	m.mu.Unlock()

	ready := make(chan struct{})
	started := make(chan error, 1)
	writer := &boundedReadyWriter{writer: file, limit: maxBytes, ready: ready}
	go func() {
		defer cancel()
		defer stopManagerCancellation()
		captureErr := m.backend.Capture(captureContext, spec, writer)
		_ = file.Close()
		select {
		case <-ready:
		default:
			started <- captureErr
		}
		m.finish(id, writer.written, captureErr, captureContext.Err())
	}()

	select {
	case <-ready:
		m.mu.Lock()
		if item.record.State == "starting" {
			item.record.State = "capturing"
		}
		result := item.record
		m.mu.Unlock()
		return result, nil
	case startErr := <-started:
		if startErr == nil {
			startErr = errors.New("capture ended before writing a PCAP header")
		}
		return Record{}, startErr
	case <-requestContext.Done():
		cancel()
		return Record{}, requestContext.Err()
	case <-time.After(15 * time.Second):
		cancel()
		return Record{}, errors.New("capture did not become ready within 15 seconds")
	}
}

func (m *Manager) Get(id string) (Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	item := m.captures[id]
	if item == nil {
		return Record{}, ErrNotFound
	}
	result := item.record
	if information, err := os.Stat(item.path); err == nil {
		result.Bytes = information.Size()
	}
	return result, nil
}

func (m *Manager) Stop(id string) (Record, error) {
	m.mu.RLock()
	item := m.captures[id]
	if item == nil {
		m.mu.RUnlock()
		return Record{}, ErrNotFound
	}
	cancel := item.cancel
	m.mu.RUnlock()
	cancel()
	return m.Get(id)
}

func (m *Manager) Open(id string) (*os.File, Record, error) {
	record, err := m.Get(id)
	if err != nil {
		return nil, Record{}, err
	}
	if record.State == "starting" || record.State == "capturing" {
		return nil, Record{}, ErrStillRunning
	}
	m.mu.RLock()
	path := m.captures[id].path
	m.mu.RUnlock()
	file, err := os.Open(path) //nolint:gosec // path is generated internally.
	return file, record, err
}

func (m *Manager) Delete(id string) error {
	m.mu.Lock()
	item := m.captures[id]
	if item == nil {
		m.mu.Unlock()
		return ErrNotFound
	}
	if item.record.State == "starting" || item.record.State == "capturing" {
		m.mu.Unlock()
		return ErrStillRunning
	}
	delete(m.captures, id)
	m.mu.Unlock()
	return os.Remove(item.path)
}

func (m *Manager) RunReaper(ctx context.Context) {
	interval := min(time.Minute, m.config.Storage.Retention.Duration/2)
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.reap(now)
		}
	}
}

func (m *Manager) Close() { m.cancel() }

func (m *Manager) normalize(request StartRequest) (kube.CaptureSpec, int64, error) {
	if request.Node == "" || request.Interface == "" {
		return kube.CaptureSpec{}, 0, fmt.Errorf("%w: node and interface are required", ErrInvalid)
	}
	if len(request.Node) > 253 || !interfaceNamePattern.MatchString(request.Interface) {
		return kube.CaptureSpec{}, 0, fmt.Errorf("%w: node or interface has an invalid format", ErrInvalid)
	}
	if request.DurationSeconds < 0 || request.PacketLimit < 0 || request.MaxBytes < 0 || request.SnapLength < 0 {
		return kube.CaptureSpec{}, 0, fmt.Errorf("%w: capture limits cannot be negative", ErrInvalid)
	}
	var duration time.Duration
	switch {
	case request.DurationSeconds == 0:
		duration = min(15*time.Second, m.config.Limits.MaxDuration.Duration)
	case request.DurationSeconds > int(m.config.Limits.MaxDuration.Duration/time.Second):
		return kube.CaptureSpec{}, 0, fmt.Errorf("%w: duration exceeds configured maximum", ErrInvalid)
	default:
		duration = time.Duration(request.DurationSeconds) * time.Second
	}
	packetLimit := request.PacketLimit
	if packetLimit <= 0 {
		packetLimit = m.config.Limits.MaxPackets
	}
	if packetLimit > m.config.Limits.MaxPackets {
		return kube.CaptureSpec{}, 0, fmt.Errorf("%w: packet limit exceeds configured maximum", ErrInvalid)
	}
	snapLength := request.SnapLength
	if snapLength <= 0 {
		snapLength = m.config.Limits.MaxSnapLength
	}
	if snapLength < 64 || snapLength > m.config.Limits.MaxSnapLength {
		return kube.CaptureSpec{}, 0, fmt.Errorf("%w: snaplen is outside the configured range", ErrInvalid)
	}
	maxBytes := request.MaxBytes
	if maxBytes <= 0 {
		maxBytes = m.config.Limits.MaxBytes
	}
	if maxBytes > m.config.Limits.MaxBytes {
		return kube.CaptureSpec{}, 0, fmt.Errorf("%w: byte limit exceeds configured maximum", ErrInvalid)
	}
	return kube.CaptureSpec{
		Node: request.Node, Interface: request.Interface, Duration: duration,
		PacketLimit: packetLimit, SnapLength: snapLength,
	}, maxBytes, nil
}

func (m *Manager) finish(id string, written int64, captureErr, contextErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	item := m.captures[id]
	if item == nil {
		return
	}
	item.record.Bytes = written
	item.record.CompletedAt = time.Now().UTC()
	switch {
	case errors.Is(captureErr, ErrMaxBytes):
		item.record.State = "completed"
		item.record.Error = "maximum capture size reached"
	case contextErr != nil:
		item.record.State = "stopped"
	case captureErr != nil:
		item.record.State = "failed"
		item.record.Error = captureErr.Error()
	default:
		item.record.State = "completed"
	}
	if m.running > 0 {
		m.running--
	}
}

func (m *Manager) reap(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, item := range m.captures {
		if item.record.CompletedAt.IsZero() || now.Sub(item.record.CompletedAt) < m.config.Storage.Retention.Duration {
			continue
		}
		_ = os.Remove(item.path)
		delete(m.captures, id)
	}
}

func newID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

type boundedReadyWriter struct {
	writer  io.Writer
	limit   int64
	written int64
	ready   chan struct{}
	once    sync.Once
}

func (w *boundedReadyWriter) Write(value []byte) (int, error) {
	remaining := w.limit - w.written
	if remaining <= 0 {
		return 0, ErrMaxBytes
	}
	if int64(len(value)) > remaining {
		value = value[:remaining]
	}
	written, err := w.writer.Write(value)
	w.written += int64(written)
	if w.written >= pcapHeaderLength {
		w.once.Do(func() { close(w.ready) })
	}
	if err == nil && int64(written) >= remaining {
		err = ErrMaxBytes
	}
	return written, err
}
