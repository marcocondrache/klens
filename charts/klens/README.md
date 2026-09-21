# klens

![Version](https://img.shields.io/static/v1?label=Version&message=0.1.0&color=informational&style=flat-square)
![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square)
![AppVersion](https://img.shields.io/static/v1?label=AppVersion&message=0.51.0&color=informational&style=flat-square)

Kafka UI for inspecting topics, messages, consumer groups, and schemas

**Homepage:** <https://github.com/marcocondrache/klens>

## Usage

Install from GHCR. `config` is the same YAML the process loads in Docker.

```sh
helm install klens oci://ghcr.io/marcocondrache/charts/klens \
  -n klens --create-namespace -f my-values.yaml
```

From a checkout:

```sh
helm install klens ./charts/klens -n klens --create-namespace -f my-values.yaml
```

Put Kafka and OIDC credentials in `secret.existingSecret` or `secret.stringData`.
Reference them in `config` as `${VAR}`. Mount Kafka PEM files with `volumes`
and `volumeMounts`. Set a Secret `defaultMode` of 0400 on the client key.

`bind` must be a SocketAddr the Service can reach. A loopback address fails render.
A `config.bind` port that does not match `service.port` also fails render.

`/health` is process liveness after listen. `/ready` waits for the first topology
poll on every configured cluster. `helm test` curls `/health`.

replicaCount greater than 1 is legal. Login sessions stay in process memory
and do not stick across pods.

There is no `/metrics` route. This chart does not create a ServiceMonitor.

Expose the UI with `ingress` or a Gateway API `httpRoute`. Use a Prefix `/`
path so SPA routes, `/auth/*`, and `/api` reach
the pod.

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| marcocondrache |  | <https://github.com/marcocondrache> |

## Source Code

* <https://github.com/marcocondrache/klens>

## Requirements

Kubernetes: `>=1.25.0-0`

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` | Affinity rules for pod scheduling (templated). |
| config | object | `{"bind":"0.0.0.0:8080","clusters":[],"log_level":"info"}` | klens config file as a map. The binary validates this YAML, not the chart. Lane cadence is per cluster (`clusters[].ingest`). |
| deploymentAnnotations | object | `{}` | Annotations added to the Deployment. |
| env | object | `{}` | Extra environment variables as a map (templated). |
| envFrom | list | `[]` | Extra envFrom sources (templated). |
| existingConfigMap | string | `""` | Mount an existing ConfigMap with a `config.yaml` key instead of the inline `config`. |
| extraEnv | list | `[]` | Extra environment variables as a raw list (templated). |
| fullnameOverride | string | `""` | Override the full release name. |
| httpRoute.additionalRules | list | `[]` | Custom rules prepended before the default rule (templated). |
| httpRoute.annotations | object | `{}` | HTTPRoute annotations. |
| httpRoute.apiVersion | string | `""` | HTTPRoute apiVersion. Empty defaults to gateway.networking.k8s.io/v1. |
| httpRoute.enabled | bool | `false` | Expose the UI via a Gateway API HTTPRoute. |
| httpRoute.filters | list | `[]` | Filters applied to the default rule. |
| httpRoute.hostnames | list | `[]` | Hostnames matched against the Host header (templated). |
| httpRoute.httpsRedirect | bool | `false` | Redirect HTTP to HTTPS (301) instead of routing to the backend. |
| httpRoute.kind | string | `""` | HTTPRoute kind. Empty defaults to HTTPRoute. |
| httpRoute.labels | object | `{}` | HTTPRoute labels. |
| httpRoute.matches | list | `[{"path":{"type":"PathPrefix","value":"/"}}]` | Match conditions for the default rule. |
| httpRoute.parentRefs | list | `[]` | Gateways this route attaches to. |
| image.digest | string | `""` | Pin the image by digest (`sha256:…`). When set, it overrides the tag. |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy. |
| image.repository | string | `"ghcr.io/marcocondrache/klens"` | Image repository. |
| image.tag | string | `""` | Overrides the image tag. Empty uses the chart appVersion. |
| imagePullSecrets | list | `[]` | Image pull secrets for private registries. |
| ingress.annotations | object | `{}` | Ingress annotations. |
| ingress.className | string | `""` | IngressClass name. |
| ingress.enabled | bool | `false` | Expose the UI via an Ingress. Use a Prefix `/` path so SPA routes and `/api` reach the pod. |
| ingress.hosts | list | `[{"host":"klens.example.com","paths":[{"path":"/","pathType":"Prefix"}]}]` | Ingress hosts and their paths. |
| ingress.tls | list | `[]` | Ingress TLS configuration. |
| livenessProbe | object | `{"httpGet":{"path":"/health","port":"http"},"periodSeconds":20}` | Liveness probe. Targets `/health` after startup succeeds. |
| nameOverride | string | `""` | Override the chart name used in resource names. |
| nodeSelector | object | `{}` | Node selector for pod scheduling (templated). |
| podAnnotations | object | `{}` | Annotations added to the pod. |
| podLabels | object | `{}` | Labels added to the pod. |
| podSecurityContext | object | `{"fsGroup":65532,"runAsGroup":65532,"runAsNonRoot":true,"runAsUser":65532,"seccompProfile":{"type":"RuntimeDefault"}}` | Pod-level securityContext (non-root uid/gid 65532, RuntimeDefault seccomp). |
| priorityClassName | string | `""` | PriorityClass for the pod. Empty uses the cluster default. |
| readinessProbe | object | `{"httpGet":{"path":"/ready","port":"http"},"periodSeconds":10}` | Readiness probe. Targets `/ready` after every configured cluster has a topology poll. |
| replicaCount | int | `1` | Replica count. Sessions live in process memory and do not stick across pods. |
| resources | object | `{}` | Pod resource requests and limits. |
| secret.existingSecret | string | `""` | Existing Secret whose keys are injected as environment variables. |
| secret.stringData | object | `{}` | Inline Secret keys. Ignored when `existingSecret` is set. |
| securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true}` | Container securityContext (no privilege escalation, read-only root, drop ALL). |
| service.annotations | object | `{}` | Service annotations. |
| service.port | int | `8080` | Service port. Must match the port in `config.bind` when the chart owns the ConfigMap. |
| service.type | string | `"ClusterIP"` | Service type. |
| serviceAccount.annotations | object | `{}` | Annotations for the ServiceAccount. |
| serviceAccount.automount | bool | `false` | Automount the ServiceAccount API token. Off because klens does not call the Kubernetes API. |
| serviceAccount.create | bool | `true` | Create a ServiceAccount. |
| serviceAccount.name | string | `""` | ServiceAccount name. Empty uses the chart fullname. |
| startupProbe | object | `{"failureThreshold":36,"httpGet":{"path":"/health","port":"http"},"periodSeconds":5}` | Startup probe. Targets `/health` until the process binds. |
| terminationGracePeriodSeconds | int | `30` | Grace period for a clean shutdown. |
| tests.image.pullPolicy | string | `"IfNotPresent"` | `helm test` image pull policy. |
| tests.image.repository | string | `"mirror.gcr.io/curlimages/curl"` | `helm test` connection-pod image. A gcr-mirrored curl so the test never pulls from Docker Hub. |
| tests.image.tag | string | `"8.22.0@sha256:58adaa4e8dca9c988bae2aba4ab3434a0bb2da16bbe3f92dec39ec7785166777"` | `helm test` image, pinned as `tag@sha256:digest`. |
| tolerations | list | `[]` | Tolerations for pod scheduling (templated). |
| topologySpreadConstraints | list | `[]` | Topology spread constraints (templated). |
| volumeMounts | list | `[]` | Additional volume mounts on the container. |
| volumes | list | `[]` | Additional volumes on the Deployment. Use these for Kafka TLS PEMs. Set Secret `defaultMode` to 0400 so the client key is not group- or world-readable. |
