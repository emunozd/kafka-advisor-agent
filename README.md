# Kafka Advisor Agent

> Agentic AI tool deployed on Red Hat OpenShift AI that analyzes AMQ Streams CRD YAML
> and returns optimization recommendations based on the **Kafka Optimization Theorem**,
> powered by **Llama 3.2 3B FP8** via vLLM ServingRuntime on GPU.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Prerequisites](#prerequisites)
3. [Repository Structure](#repository-structure)
4. [Deployment Guide](#deployment-guide)
   - [Step 1 — Verify prerequisites](#step-1--verify-prerequisites)
   - [Step 2 — Create the namespace](#step-2--create-the-namespace)
   - [Step 3 — Apply foundation manifests](#step-3--apply-foundation-manifests)
   - [Step 4 — Create the model PVC](#step-4--create-the-model-pvc)
   - [Step 5 — Deploy the ServingRuntime](#step-5--deploy-the-servingruntime)
   - [Step 6 — Deploy the InferenceService](#step-6--deploy-the-inferenceservice)
   - [Step 7 — Verify the model endpoint](#step-7--verify-the-model-endpoint)
   - [Step 8 — Build the Quarkus app on OCP](#step-8--build-the-quarkus-app-on-ocp)
   - [Step 9 — Deploy the Quarkus app](#step-9--deploy-the-quarkus-app)
   - [Step 10 — Access the UI](#step-10--access-the-ui)
5. [Updating the Knowledge Base](#updating-the-knowledge-base)
6. [Supported CRD Kinds](#supported-crd-kinds)
7. [Kafka Optimization Theorem Reference](#kafka-optimization-theorem-reference)
8. [Stopping and Starting the Agent](#stopping-and-starting-the-agent)
9. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
User / SRE  ──HTTPS──►  Quarkus REST App  (OCP Route)
                               │
                    reads ─────┤── ConfigMap: kafka-optimization-prompt
                    reads ─────┤── Secret:    model endpoint + key
                               │
                  REST ────────►  RHOAI InferenceService
                                     vLLM ServingRuntime
                                     GPU Worker Node  (nvidia.com/gpu: 1)
                                     PVC: model weights (OCI image pull)
```

All components run **inside the OpenShift cluster** — zero external dependencies at runtime.

---

## Prerequisites

### Platform versions

| Component | Minimum version | Notes |
|---|---|---|
| Red Hat OpenShift Container Platform | **4.14+** | Tested on 4.19 |
| Red Hat OpenShift AI (RHOAI) | **2.x** | Tested on 2.25.3 |
| Node Feature Discovery (NFD) Operator | **4.x** | Required for GPU node labeling |
| NVIDIA GPU Operator | **23.x+** | Tested on 25.10.1 |

### Required operators

All operators must be in `Succeeded` state before starting. Verify with:

```bash
oc get csv -n redhat-ods-operator   | grep rhods
oc get csv -n openshift-nfd         | grep nfd
oc get csv -n nvidia-gpu-operator   | grep gpu
```

Expected output for each: `Succeeded`

### Required infrastructure

| Resource | Requirement |
|---|---|
| GPU worker node | At least 1 node with `nvidia.com/gpu: 1` |
| GPU VRAM | Minimum 8 GB (Llama 3.2 3B FP8 ≈ 6 GB) |
| Storage | StorageClass with `ReadWriteOnce`, minimum 15 Gi |
| Network | Cluster must reach `quay.io` (OCI image pull at deploy time) |

Verify GPU visibility:

```bash
oc get nodes -o custom-columns="NAME:.metadata.name,GPU:.status.capacity.nvidia\.com/gpu" \
  | grep -v "<none>"
```

### Local tooling (developer machine)

| Tool | Version | Purpose |
|---|---|---|
| `oc` CLI | 4.x | Cluster interaction |
| `git` | any | Source control |
| Java JDK | 17 or 21 | Local Quarkus development |
| Quarkus CLI | 3.x | Project scaffolding (optional) |

---

## Repository Structure

```
kafka-advisor-agent/
│
├── README.md
│
├── manifests/                          ← OCP manifests (apply in order)
│   ├── kustomization.yaml              ← Kustomize root (phased deployment)
│   │
│   ├── base/
│   │   ├── 00-namespace.yaml           ← Namespace + RHOAI dashboard labels
│   │   ├── 01-configmap-prompt.yaml    ← Kafka Optimization Theorem (system prompt)
│   │   ├── 02-secret-model.yaml        ← Model endpoint URL + name
│   │   └── 07-rbac.yaml               ← ServiceAccount + ClusterRole for CRD read
│   │
│   ├── rhoai/
│   │   ├── 03-serving-runtime.yaml     ← vLLM ServingRuntime (GPU, Red Hat image)
│   │   ├── 04-model-pvc-and-download-job.yaml  ← PVC for model weights
│   │   └── 05-inference-service.yaml   ← InferenceService (OCI model pull)
│   │
│   └── app/
│       └── 06-quarkus-app.yaml         ← Deployment + Service + Route
│
└── kafka-advisor-app/                  ← Quarkus 3 source code
    ├── pom.xml                         ← Maven deps: quarkus-rest, langchain4j-openai, qute
    ├── src/main/
    │   ├── java/com/redhat/kafka/advisor/
    │   │   ├── KafkaAdvisorResource.java   ← REST endpoint /analyze
    │   │   └── KafkaAdvisorService.java    ← Placeholder (logic in Resource)
    │   ├── resources/
    │   │   ├── application.properties      ← LangChain4j config
    │   │   ├── system-prompt.txt           ← Fallback prompt (dev mode)
    │   │   └── templates/
    │   │       └── advisor.html            ← Qute frontend template
    │   └── docker/
    │       └── Dockerfile.jvm             ← JVM Dockerfile (used by BuildConfig)
    └── mvnw                               ← Maven wrapper
```

---

## Deployment Guide

### Step 1 — Verify prerequisites

```bash
# 1a. RHOAI operator
oc get csv -n redhat-ods-operator | grep rhods
# Expected: Succeeded

# 1b. DataScienceCluster ready
oc get datasciencecluster -n redhat-ods-applications
# Expected: READY=True

# 1c. NFD operator
oc get csv -n openshift-nfd | grep nfd
# Expected: Succeeded

# 1d. GPU operator
oc get csv -n nvidia-gpu-operator | grep gpu
# Expected: Succeeded

# 1e. GPU visible on a node
oc get nodes -o custom-columns="NAME:.metadata.name,GPU:.status.capacity.nvidia\.com/gpu" \
  | grep -v "<none>"
# Expected: at least one node with GPU count > 0
```

All five checks must pass before continuing.

---

### Step 2 — Create the namespace

```bash
oc apply -f manifests/base/00-namespace.yaml
```

This creates the `kafka-advisor` namespace with the labels required for RHOAI dashboard
integration (`opendatahub.io/dashboard: "true"`) and disables ModelMesh
(`modelmesh-enabled: "false"`).

Verify:

```bash
oc get namespace kafka-advisor
```

> **Note:** If you already ran `oc new-project kafka-advisor`, the `apply` will patch the
> missing annotations automatically. The warning about missing
> `last-applied-configuration` is normal and can be ignored.

---

### Step 3 — Apply foundation manifests

```bash
oc apply -f manifests/base/01-configmap-prompt.yaml
oc apply -f manifests/base/02-secret-model.yaml
oc apply -f manifests/base/07-rbac.yaml
```

What each file creates:

| File | Resource | Purpose |
|---|---|---|
| `01-configmap-prompt.yaml` | `ConfigMap/kafka-optimization-prompt` | System prompt with the Kafka Optimization Theorem. Mounted into the Quarkus app at `/deployments/config/prompt/system-prompt.txt`. Update this file to tune agent behavior without rebuilding. |
| `02-secret-model.yaml` | `Secret/kafka-advisor-model-secret` | Model API URL, API key (empty for in-cluster vLLM), and model name. |
| `07-rbac.yaml` | `ServiceAccount` + `ClusterRole` + `ClusterRoleBinding` | Allows the Quarkus app to `get/list/watch` all AMQ Streams CRDs cluster-wide (used by future auto-discovery feature). |

Verify:

```bash
oc get configmap,secret,serviceaccount -n kafka-advisor | grep kafka-advisor
```

---

### Step 4 — Create the model PVC

```bash
oc apply -f manifests/rhoai/04-model-pvc-and-download-job.yaml
```

This creates a 15 Gi `PersistentVolumeClaim` named `kafka-advisor-model-pvc`.

> **Important:** The PVC will remain in `Pending` state until the InferenceService Pod
> is scheduled. This is expected behavior with `volumeBindingMode: WaitForFirstConsumer`.

Verify:

```bash
oc get pvc -n kafka-advisor
# STATUS: Pending  ← normal at this stage
```

> **StorageClass note:** The manifest uses `crc-csi-hostpath-provisioner` (CRC default).
> For other environments adjust `storageClassName`:
> - ROSA / ODF: `ocs-storagecluster-ceph-rbd`
> - AWS: `gp3-csi`
> - Check available classes: `oc get storageclass`

---

### Step 5 — Deploy the ServingRuntime

```bash
oc apply -f manifests/rhoai/03-serving-runtime.yaml
```

This creates a `ServingRuntime` named `kafka-advisor-vllm-runtime` that uses the
official Red Hat vLLM image:

```
registry.redhat.io/rhoai/odh-vllm-cuda-rhel9@sha256:5b86924790aeb996a7e3b7f9f4c8a3a676a83cd1d7484ae584101722d362c69b
```

Key vLLM arguments configured:

| Argument | Value | Reason |
|---|---|---|
| `--dtype` | `auto` | Lets vLLM detect FP8 quantization from model weights automatically |
| `--max-model-len` | `4096` | Fits within GPU VRAM for Llama 3.2 3B FP8 |
| `--tensor-parallel-size` | `1` | Single GPU deployment |
| `--served-model-name` | `llama32-3b-instruct-fp8` | Must match `MODEL_NAME` in the Secret |

> **Image note:** The SHA256 digest is pinned to the version tested with RHOAI 2.25.
> To find the correct image for your RHOAI version:
> ```bash
> oc get servingruntime -A -o jsonpath='{range .items[*]}{.spec.containers[*].image}{"\n"}{end}' \
>   | grep vllm | sort -u
> ```

Verify:

```bash
oc get servingruntime -n kafka-advisor
# Expected: kafka-advisor-vllm-runtime   vLLM   kserve-container
```

---

### Step 6 — Deploy the InferenceService

```bash
oc apply -f manifests/rhoai/05-inference-service.yaml
```

This creates a `InferenceService` named `kafka-advisor-model` that:

- Uses `RawDeployment` mode (plain OCP Deployment, no Knative required)
- Pulls the model weights from the OCI image `oci://quay.io/rhoai-genaiops/llama32-3b-instruct-fp8`
- Requests 1 GPU, 8 Gi RAM, 2 CPU cores

Monitor the Pod startup — there are three phases:

```bash
oc get pods -n kafka-advisor -w
```

| Phase | Container status | Description |
|---|---|---|
| 1 | `Init: 0/1` | `modelcar-init` pulling the OCI model image (~3-5 min) |
| 2 | `PodInitializing` | Model weights being extracted to `/mnt/models` |
| 3 | `Running 1/2` | vLLM loading weights into GPU (~2 min) |
| 4 | `Running 2/2` | Ready — vLLM serving requests |

Wait for `READY: True` on the InferenceService:

```bash
oc get inferenceservice -n kafka-advisor
# Expected: READY=True  URL=http://kafka-advisor-model-predictor.kafka-advisor.svc.cluster.local
```

> **Tip:** Total cold-start time is approximately 5-10 minutes on first deploy.
> Subsequent restarts are faster because the OCI image is cached on the node.

---

### Step 7 — Verify the model endpoint

Test the OpenAI-compatible API from inside the cluster:

```bash
# List available models
oc exec deployment/kafka-advisor-model-predictor -n kafka-advisor -- \
  curl -s http://localhost:8080/v1/models

# Test inference
oc exec deployment/kafka-advisor-model-predictor -n kafka-advisor -- \
  curl -s http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama32-3b-instruct-fp8",
    "max_tokens": 80,
    "messages": [{"role":"user","content":"What is Kafka replication.factor?"}]
  }'
```

Expected: a JSON response with `"finish_reason": "stop"` and a coherent answer.

---

### Step 8 — Build the Quarkus app on OCP

First, import the Java 21 builder image if it is not already available:

```bash
oc import-image java:openjdk-21-ubi9 \
  --from=registry.access.redhat.com/ubi9/openjdk-21 \
  --confirm \
  -n openshift
```

Create the ImageStream and BuildConfig:

```bash
oc apply -f - <<'EOF'
apiVersion: image.openshift.io/v1
kind: ImageStream
metadata:
  name: kafka-advisor-app
  namespace: kafka-advisor
  labels:
    app.kubernetes.io/part-of: kafka-advisor
spec:
  lookupPolicy:
    local: true
EOF

oc apply -f - <<'EOF'
apiVersion: build.openshift.io/v1
kind: BuildConfig
metadata:
  name: kafka-advisor-app
  namespace: kafka-advisor
  labels:
    app.kubernetes.io/part-of: kafka-advisor
spec:
  source:
    type: Git
    git:
      uri: https://github.com/emunozd/kafka-advisor-agent.git
      ref: main
    contextDir: kafka-advisor-app
  strategy:
    type: Source
    sourceStrategy:
      from:
        kind: ImageStreamTag
        namespace: openshift
        name: java:openjdk-21-ubi9
      env:
        - name: MAVEN_ARGS
          value: "package -DskipTests -Dquarkus.package.type=fast-jar"
        - name: MAVEN_ARGS_APPEND
          value: "-Dquarkus.container-image.build=false"
      forcePull: true
  output:
    to:
      kind: ImageStreamTag
      name: kafka-advisor-app:latest
  triggers:
    - type: ConfigChange
EOF
```

Start the build and follow the logs:

```bash
oc start-build kafka-advisor-app -n kafka-advisor --follow
```

The S2I build will:
1. Clone the GitHub repository
2. Run `mvn package` inside the builder image
3. Produce a fast-jar Quarkus application
4. Push the resulting image to the internal OCP registry

Build time: approximately 3-5 minutes (first build downloads Maven dependencies).

Verify the image was pushed:

```bash
oc get imagestreamtag kafka-advisor-app:latest -n kafka-advisor
```

---

### Step 9 — Deploy the Quarkus app

```bash
oc apply -f manifests/app/06-quarkus-app.yaml
```

This creates three resources:

| Resource | Name | Purpose |
|---|---|---|
| `Deployment` | `kafka-advisor-app` | Runs the Quarkus JVM container |
| `Service` | `kafka-advisor-app` | Internal ClusterIP on port 80 → 8080 |
| `Route` | `kafka-advisor-app` | TLS edge-terminated HTTPS public URL |

The Deployment injects these environment variables from the Secret:

| Env var | Secret key | Value |
|---|---|---|
| `QUARKUS_LANGCHAIN4J_OPENAI_BASE_URL` | `MODEL_API_URL` | `http://kafka-advisor-model-predictor.kafka-advisor.svc.cluster.local:8080/v1` |
| `QUARKUS_LANGCHAIN4J_OPENAI_API_KEY` | `MODEL_API_KEY` | `dummy` (vLLM in-cluster needs a non-empty value) |
| `QUARKUS_LANGCHAIN4J_OPENAI_CHAT_MODEL_MODEL_NAME` | `MODEL_NAME` | `llama32-3b-instruct-fp8` |

Monitor startup:

```bash
oc get pods -n kafka-advisor -w
# Expected: kafka-advisor-app-xxxxx   1/1   Running
```

Check health:

```bash
oc exec deployment/kafka-advisor-app -n kafka-advisor -- \
  curl -s http://localhost:8080/q/health/ready
```

---

### Step 10 — Access the UI

Get the public URL:

```bash
oc get route kafka-advisor-app -n kafka-advisor \
  -o jsonpath='https://{.spec.host}'
```

Open the URL in a browser. You will see the **Kafka Advisor Agent** UI with:

- Five optimization goal buttons: Throughput, Durability, Availability, Low Latency, Balanced
- Input panel for pasting CRD YAML
- Optimized YAML output panel
- Agent recommendations panel

Paste any of the supported CRD kinds, select a goal and click **Analyze & Optimize**.

---

## Updating the Knowledge Base

The Kafka Optimization Theorem is stored in a ConfigMap — no rebuild required to update it:

```bash
oc edit configmap kafka-optimization-prompt -n kafka-advisor
```

The Quarkus app reads the file at `/deployments/config/prompt/system-prompt.txt` on
every request, so changes take effect immediately without restarting the Pod.

To update from your local file:

```bash
oc create configmap kafka-optimization-prompt \
  --from-file=system-prompt.txt=manifests/base/system-prompt.txt \
  -n kafka-advisor \
  --dry-run=client -o yaml | oc apply -f -
```

---

## Supported CRD Kinds

| Kind | API Group | Analyzed fields | Example optimizations |
|---|---|---|---|
| `Kafka` | `kafka.strimzi.io/v1beta2` | Broker config, listeners, KRaft/ZK settings | `num.io.threads`, `log.retention.ms`, `default.replication.factor` |
| `KafkaTopic` | `kafka.strimzi.io/v1beta2` | `partitions`, `replicas`, topic config map | `partitions`, `min.insync.replicas`, `retention.ms`, `compression.type` |
| `KafkaUser` | `kafka.strimzi.io/v1beta2` | Quotas, ACLs, authentication type | `producerByteRate`, `consumerByteRate`, `requestPercentage` |
| `KafkaMirrorMaker2` | `kafka.strimzi.io/v1beta2` | Replication policy, offset sync, connectors | `replication.factor`, `refresh.topics.interval.seconds`, heartbeat topics |

---

## Kafka Optimization Theorem Reference

The agent's knowledge base is built on the four-quadrant trade-off framework:

```
                    DURABILITY
                        ▲
          replication.factor+     acks=all
          min.insync.replicas+    enable.idempotence=true
                        │
  LATENCY ◄─────────────┼─────────────► THROUGHPUT
  batch.size-            │              batch.size+
  linger.ms=0            │              linger.ms+
  fetch.min.bytes=1      │              fetch.min.bytes+
  partitions-            │              partitions+
                        │
          replication.factor-     acks=0
          min.insync.replicas-    max.poll.records+
                        ▼
                   AVAILABILITY
```

Each optimization goal adjusts parameters toward the corresponding quadrant while
noting the trade-offs with the opposing quadrant.

---

## Stopping and Starting the Agent

The agent can be paused to free up GPU and CPU resources for other workloads,
without losing the model weights stored in the PVC.

### Stop (free the GPU)
```bash
# Scale the Quarkus app to zero
oc scale deployment/kafka-advisor-app -n kafka-advisor --replicas=0

# Delete the InferenceService to release the GPU
oc delete inferenceservice kafka-advisor-model -n kafka-advisor
```

Verify the GPU is free:
```bash
oc get node crc -o jsonpath='{.status.allocatable.nvidia\.com/gpu}'
# Expected: 1
```

> The PVC with model weights is preserved — the next start will be faster
> because the OCI image is already cached on the node.

### Start
```bash
# 1. Recreate the InferenceService
oc apply -f manifests/rhoai/05-inference-service.yaml

# 2. Wait for the model to be Ready (~3-5 min)
oc wait inferenceservice/kafka-advisor-model -n kafka-advisor \
  --for=condition=ready --timeout=10m

# 3. Scale the Quarkus app back up
oc scale deployment/kafka-advisor-app -n kafka-advisor --replicas=1

# 4. Get the URL
oc get route kafka-advisor-app -n kafka-advisor \
  -o jsonpath='https://{.spec.host}'
```

### Verify full status
```bash
oc get pods,inferenceservice,route -n kafka-advisor
```

Expected output when fully running:

| Resource | Name | Status |
|---|---|---|
| Pod | `kafka-advisor-model-predictor-xxx` | `2/2 Running` |
| Pod | `kafka-advisor-app-xxx` | `1/1 Running` |
| InferenceService | `kafka-advisor-model` | `READY=True` |
| Route | `kafka-advisor-app` | HTTPS URL available |

---

## Troubleshooting

### InferenceService stuck in `READY: False`

```bash
# Check Pod status
oc get pods -n kafka-advisor

# Check events on the Pod
oc describe pod -n kafka-advisor \
  -l serving.kserve.io/inferenceservice=kafka-advisor-model | grep -A 20 Events

# Check vLLM logs
oc logs deployment/kafka-advisor-model-predictor \
  -n kafka-advisor -c kserve-container
```

Common causes:

| Symptom | Cause | Fix |
|---|---|---|
| `ImagePullBackOff` on `kserve-container` | Wrong vLLM image tag | Get correct image: `oc get servingruntime -A -o jsonpath='{..image}' \| tr ' ' '\n' \| grep vllm` |
| `Insufficient nvidia.com/gpu` | Another Pod is using the GPU | `oc get pods -A \| grep -i gpu` and delete idle GPU Pods |
| `invalid choice: float8` for `--dtype` | Old ServingRuntime cached | Set `--dtype=auto` and delete the Pod to force recreate |
| PVC stuck in `Pending` | No matching StorageClass | Check `oc get storageclass` and update `storageClassName` in `04-model-pvc-and-download-job.yaml` |

### Quarkus app CrashLoopBackOff

```bash
oc logs deployment/kafka-advisor-app -n kafka-advisor
```

Common causes:

| Error message | Fix |
|---|---|
| `config property quarkus.langchain4j.openai.api-key is defined as empty String` | Patch Secret: `MODEL_API_KEY` must be `dummy`, not empty |
| `Connection refused: kafka-advisor-model-predictor...:80` | Secret `MODEL_API_URL` missing port — must be `:8080/v1` |
| `No such file: system-prompt.txt` | ConfigMap not mounted — verify `01-configmap-prompt.yaml` was applied |

### Rebuilding after a code change

```bash
# On your developer machine
git add .
git commit -m "your change"
git push

# On the OCP cluster
oc start-build kafka-advisor-app -n kafka-advisor --follow
oc rollout restart deployment/kafka-advisor-app -n kafka-advisor
```

### Checking full namespace status

```bash
oc get all -n kafka-advisor
```
