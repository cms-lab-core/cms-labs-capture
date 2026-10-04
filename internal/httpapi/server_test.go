package httpapi

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cms-lab-core/cms-labs-capture/internal/capture"
	"github.com/cms-lab-core/cms-labs-capture/internal/config"
	"github.com/cms-lab-core/cms-labs-capture/internal/kube"
)

type apiBackend struct{}

func (apiBackend) Targets(context.Context) ([]kube.Target, error) {
	return []kube.Target{{Node: "r1", Interfaces: []string{"eth1"}}}, nil
}

func (apiBackend) Capture(ctx context.Context, _ kube.CaptureSpec, output io.Writer) error {
	header := make([]byte, 24)
	binary.LittleEndian.PutUint32(header, 0xa1b2c3d4)
	if _, err := output.Write(header); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestCaptureAPIFlow(t *testing.T) {
	t.Parallel()
	manager, err := capture.NewManager(context.Background(), apiBackend{}, config.Config{
		Storage: config.Storage{Path: t.TempDir()},
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(manager, "X-CMS-Identity", nil)
	if err != nil {
		t.Fatal(err)
	}

	targetResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(targetResponse, httptest.NewRequest(http.MethodGet, "/v1/targets", nil))
	if targetResponse.Code != http.StatusOK || !bytes.Contains(targetResponse.Body.Bytes(), []byte(`"eth1"`)) {
		t.Fatalf("targets response = %d %s", targetResponse.Code, targetResponse.Body.String())
	}

	startResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(startResponse, httptest.NewRequest(
		http.MethodPost, "/v1/captures", bytes.NewBufferString(`{"node":"r1","interface":"eth1"}`),
	))
	if startResponse.Code != http.StatusCreated {
		t.Fatalf("start response = %d %s", startResponse.Code, startResponse.Body.String())
	}
	var record capture.Record
	if err = json.NewDecoder(startResponse.Body).Decode(&record); err != nil {
		t.Fatal(err)
	}

	stopResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(stopResponse, httptest.NewRequest(
		http.MethodPost, "/v1/captures/"+record.ID+"/stop", nil,
	))
	if stopResponse.Code != http.StatusAccepted {
		t.Fatalf("stop response = %d %s", stopResponse.Code, stopResponse.Body.String())
	}
}

func TestCaptureAPIRejectsUnknownRequestFields(t *testing.T) {
	t.Parallel()
	manager, err := capture.NewManager(context.Background(), apiBackend{}, config.Config{
		Storage: config.Storage{Path: t.TempDir()},
	})
	if err != nil {
		t.Fatal(err)
	}
	server, _ := New(manager, "", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(
		http.MethodPost, "/v1/captures", bytes.NewBufferString(`{"node":"r1","interface":"eth1","command":"id"}`),
	))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}
