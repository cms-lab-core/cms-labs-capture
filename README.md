# cms-labs-capture

Namespace-local packet capture for CMS Labs. The project does not inject `tcpdump` into device
images. It uses the bounded `packet-capture` operation already provided by the Clabernetes
`clabwire` container and exposes it through a small asynchronous HTTP API.

## Deployment model

Install the controller once per cluster:

```sh
helm upgrade --install capture \
  oci://ghcr.io/cms-lab-core/charts/cms-labs-capture \
  --namespace cms-labs-system --create-namespace
```

A lab repository enables capture with one declaration:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cms-labs-capture-config
  namespace: $NAME
  labels:
    cms-labs.io/capture-config: "true"
data:
  config.yaml: |-
    limits:
      maxConcurrent: 2
      maxDuration: 60s
      maxBytes: 52428800
```

The controller accepts declarations only in namespaces carrying
`app.kubernetes.io/managed-by=clabgate`. It creates a Deployment, Service, namespace-only RBAC,
NetworkPolicy and bounded `emptyDir`. All generated resources are owned by the declaration
ConfigMap and disappear with it or with the lab namespace.

The browser reaches the API only through the authenticated CMS workspace proxy. Jupyter reaches the
same stable `http://cms-labs-capture:8080` Service from its own namespace. Production requires a CNI
that enforces Kubernetes NetworkPolicy; the generated policy accepts only the Clabgate workspace Pod
and the configured proxy namespace.

## API

```text
GET    /v1/targets
POST   /v1/captures
GET    /v1/captures/{id}
POST   /v1/captures/{id}/stop
GET    /v1/captures/{id}/download
DELETE /v1/captures/{id}
```

Start a capture:

```json
{
  "node": "r1",
  "interface": "eth1",
  "durationSeconds": 15,
  "packetLimit": 10000,
  "maxBytes": 52428800,
  "snaplen": 262144
}
```

The service resolves the Node UID and linked interface from current `c9s.run` resources, resolves
the current Pod and executes one fixed argv in `clabwire`. It never accepts a shell or arbitrary
command. Temporary PCAPs expire automatically. A saved PCAP should be downloaded into the Jupyter
workspace rather than retained by this service.

Capture filters are currently display filters applied by Jupyter/tshark after capture. The current
Clabernetes packet-capture contract does not yet accept a kernel BPF expression.

## Development

```sh
gofmt -l .
go vet ./...
go test -race -count=1 ./...
helm lint charts/cms-labs-capture
helm template capture charts/cms-labs-capture --namespace cms-labs-system
docker build -t cms-labs-capture:dev .
```
