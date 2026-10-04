package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/maintainer64/cms-labs-capture/internal/capture"
	"github.com/maintainer64/cms-labs-capture/internal/kube"
)

type Service interface {
	Targets(context.Context) ([]kube.Target, error)
	Start(context.Context, capture.StartRequest) (capture.Record, error)
	Get(string) (capture.Record, error)
	Stop(string) (capture.Record, error)
	Open(string) (*os.File, capture.Record, error)
	Delete(string) error
}

type Server struct {
	service        Service
	identityHeader string
	logger         *slog.Logger
}

func New(service Service, identityHeader string, logger *slog.Logger) (*Server, error) {
	if service == nil {
		return nil, errors.New("capture service is required")
	}
	if identityHeader == "" {
		identityHeader = "X-CMS-Identity"
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{service: service, identityHeader: identityHeader, logger: logger}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/targets", s.targets)
	mux.HandleFunc("POST /v1/captures", s.start)
	mux.HandleFunc("GET /v1/captures/{id}", s.get)
	mux.HandleFunc("POST /v1/captures/{id}/stop", s.stop)
	mux.HandleFunc("GET /v1/captures/{id}/download", s.download)
	mux.HandleFunc("DELETE /v1/captures/{id}", s.delete)
	return s.audit(mux)
}

func (s *Server) targets(response http.ResponseWriter, request *http.Request) {
	targets, err := s.service.Targets(request.Context())
	if err != nil {
		s.writeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"targets": targets})
}

func (s *Server) start(response http.ResponseWriter, request *http.Request) {
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 64<<10))
	decoder.DisallowUnknownFields()
	var input capture.StartRequest
	if err := decoder.Decode(&input); err != nil {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": "invalid request: " + err.Error()})
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": "request body must contain one JSON object"})
		return
	}
	record, err := s.service.Start(request.Context(), input)
	if err != nil {
		s.writeError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, record)
}

func (s *Server) get(response http.ResponseWriter, request *http.Request) {
	record, err := s.service.Get(request.PathValue("id"))
	if err != nil {
		s.writeError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, record)
}

func (s *Server) stop(response http.ResponseWriter, request *http.Request) {
	record, err := s.service.Stop(request.PathValue("id"))
	if err != nil {
		s.writeError(response, err)
		return
	}
	writeJSON(response, http.StatusAccepted, record)
}

func (s *Server) download(response http.ResponseWriter, request *http.Request) {
	file, record, err := s.service.Open(request.PathValue("id"))
	if err != nil {
		s.writeError(response, err)
		return
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			s.logger.Warn("closing capture download", "capture", record.ID, "error", closeErr)
		}
	}()
	response.Header().Set("Content-Type", "application/vnd.tcpdump.pcap")
	response.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s-%s.pcap"`, safeName(record.Node), safeName(record.Interface)))
	http.ServeContent(response, request, record.ID+".pcap", record.CompletedAt, file)
}

func (s *Server) delete(response http.ResponseWriter, request *http.Request) {
	if err := s.service.Delete(request.PathValue("id")); err != nil {
		s.writeError(response, err)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (s *Server) audit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/healthz" {
			next.ServeHTTP(response, request)
			return
		}
		identity := request.Header.Get(s.identityHeader)
		if len(identity) > 128 {
			identity = identity[:128]
		}
		s.logger.Info("capture API request", "method", request.Method, "path", request.URL.Path, "identity", identity)
		next.ServeHTTP(response, request)
	})
}

func (s *Server) writeError(response http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	message := "capture service failed"
	switch {
	case errors.Is(err, capture.ErrNotFound):
		status = http.StatusNotFound
		message = err.Error()
	case errors.Is(err, capture.ErrBusy):
		status = http.StatusTooManyRequests
		message = err.Error()
	case errors.Is(err, capture.ErrStillRunning):
		status = http.StatusConflict
		message = err.Error()
	case errors.Is(err, capture.ErrInvalid):
		status = http.StatusBadRequest
		message = err.Error()
	default:
		s.logger.Error("capture API failed", "error", err)
	}
	writeJSON(response, status, map[string]string{"error": message})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func safeName(value string) string {
	value = strings.Map(func(character rune) rune {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("._-", character) {
			return character
		}
		return '-'
	}, value)
	if value == "" {
		return "capture"
	}
	return value
}

func Shutdown(server *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return server.Shutdown(ctx)
}
