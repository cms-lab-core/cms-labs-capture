# syntax=docker/dockerfile:1

FROM golang:1.27.1-alpine AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/cms-labs-capture ./cmd/cms-labs-capture

FROM scratch

ARG VERSION=dev

LABEL org.opencontainers.image.title="cms-labs-capture" \
      org.opencontainers.image.description="Namespace-local packet capture broker for CMS Labs" \
      org.opencontainers.image.source="https://github.com/cms-lab-core/cms-labs-capture" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"

COPY --from=build /out/cms-labs-capture /cms-labs-capture

USER 65532:65532

EXPOSE 8080

ENTRYPOINT ["/cms-labs-capture"]
CMD ["serve"]
