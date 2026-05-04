# Schedulon

Go-based **distributed job scheduler** for **containerized** workloads. A single **scheduler** process holds tasks in an in-memory priority queue, persists snapshots to a database, and assigns work when **workers** call **`RequestTask`**. Workers run jobs via **`docker run`**, report status with **`ReportTaskStatus`**, and renew liveness through **`Heartbeat`**.

---

## 1. Project Overview

- **What it is:** A control plane plus worker agents for running container commands on machines that expose a Docker socket.
- **Multi-worker:** Many workers can register; each polls independently for tasks that match its advertised GPUs and optional tenant/model/memory constraints.
- **Pull-based scheduling:** Tasks are **not** pushed to workers. Workers poll **`RequestTask`**; the scheduler returns a task or `NotFound` when the queue is empty or no task fits.
- **Fault tolerance:** Each running task has a **lease** (default 30s, configurable). **`Heartbeat`** extends leases for tasks the worker still owns. Expired leases or missed heartbeats trigger **requeue** or **worker offline** handling so work can be picked up elsewhere (with the usual at-least-once caveats).

---

## 2. What It Can Do (Implemented Features)

Verified against this repository:

- **gRPC API** (`proto/scheduler.proto`, service `TrainingScheduler`): `SubmitTask`, `RequestTask`, `ReportTaskStatus`, `RegisterWorker`, `Heartbeat`, `MonitorWorker` (server stream), `ListWorkers`.
- **Reference CLI submitter** (`cmd/submit`): connects with optional TLS/mTLS flags; sends **`SubmitTaskRequest`** with **`ExecutionSpec`** (`image`, `command` as argv tokens from `--command`), **`required_gpus`**, **`priority`**, optional **`idempotency_key`**.
- **Task queue:** In-memory **priority queue** with **wait-time aging** (boosts effective priority for long-waiting tasks) and **worker–task scoring** when dequeuing for a specific worker (`pkg/scheduler/queue.go`, `priority_queue.go`, `policies/`).
- **Worker polling:** `RequestTask` with `worker_id` and optional `available_gpu_ids` to refresh availability (`pkg/scheduler/scheduler.go`).
- **Container execution:** Worker runs tasks with **`DockerExecutor`** — `docker run --rm` plus image, command, args, env, working dir, optional artifact volume (`pkg/worker/docker_executor.go`). Requires Docker on the worker host.
- **Task lifecycle:** States **`PENDING`**, **`RUNNING`**, **`COMPLETED`**, **`FAILED`**, **`CANCELED`** in proto; scheduler enforces transitions via `pkg/scheduler/lifecycle.go`.
- **Retries:** On **`FAILED`**, if `max_retries` not exhausted, scheduler moves task back to **`PENDING`** with backoff (`NotBefore`) before requeue (`ReportTaskStatus` path in `pkg/scheduler/scheduler.go`).
- **Lease reclaim:** Background ticker requeues **`RUNNING`** tasks whose lease expired; heartbeats refresh leases for tasks assigned to that worker (`pkg/scheduler/failure_detector.go`, `Heartbeat` in `scheduler.go`).
- **Worker loss:** Heartbeat timeout marks worker **`OFFLINE`** and requeues its **`RUNNING`** tasks (`failure_detector.go`).
- **Persistence:** Optional **SQLite** or **PostgreSQL** via GORM; saves workers, tasks, pending queue order, idempotency map; periodic auto-save (`cmd/server`, `pkg/scheduler/scheduler_persistence.go`, `pkg/persistence/`). **`persistence-type=file`** is not implemented (server falls back to database).
- **Metrics:** Prometheus **`/metrics`** on scheduler (default `:9091`) and worker (default `:9092`) (`pkg/metrics/`).
- **Structured logging:** Component loggers, optional log directory (`pkg/logging/`).
- **TLS / mTLS:** Server optional `--tls-cert` / `--tls-key` / `--tls-ca`; clients use `--tls-ca` and optional client cert in `submit`, `worker`, `monitor` (`pkg/security/tls.go`, `ClientGRPCDialOptions`).
- **Static bearer auth (server):** `--auth-token` registers allowed tokens; unary RPCs require `authorization` metadata when any token is set (`pkg/security/interceptors.go`). The bundled **`submit` and `worker` binaries do not send this metadata**, so enabling tokens requires a custom client or code changes.
- **Docker Compose:** `docker-compose.app.yml` — Postgres, scheduler, two workers (Docker socket mounted), Prometheus, Grafana (`deploy/docker/` configs).

**GPU / resource model:** Scheduler and worker track **GPU IDs, models, memory**, and match **`required_gpus`**, **`required_gpu_model`**, **`required_gpu_memory`** on submission. The **Docker invocation does not pass `--gpus` or device constraints**; GPU fields drive **scheduling and bookkeeping**, not automatic NVIDIA device injection into containers.

---

## 3. What It Does NOT Do (Limitations)

- **Single scheduler process:** No built-in HA, leader election, or multi-replica control plane.
- **No distributed queue:** Pending work lives in the scheduler process; the database stores **snapshots**, not a standalone queue service.
- **No worker autoscaling:** You start workers; the system does not provision them.
- **No GPU pass-through to Docker:** As above, containers are plain `docker run` unless you extend the executor.
- **No first-class “cancel task” RPC** on the scheduler: **`CANCELED`** is accepted from workers (e.g. reconciliation / worker-side paths); `MonitorWorker` responses today do not populate **`WorkerCommand`** from the scheduler, so features like **`STOP_TASK`** in the worker are not driven by the current server implementation.
- **No web UI** for job control: CLI (`cmd/submit`, `cmd/monitor`) and metrics/Grafana only.
- **No exactly-once execution:** Retries, lease expiry, and worker crashes can cause **duplicate runs** if a worker was still executing after a reclaim.
- **Submit CLI scope:** Only **`image`** and **`command`** (tokenized); proto fields like **`args`**, **`env`**, **`working_dir`**, **`artifact_path`**, **`timeout_seconds`**, **`tenant`**, **`max_retries`**, **`required_gpu_model`**, **`required_gpu_memory`** require **another gRPC client** or extending `cmd/submit`.
- **Auth + bundled clients:** Enabling **`--auth-token`** without client changes will break **`submit`** / **`worker`** / **`monitor`**.

---

## 4. Architecture

| Component | Role |
|-----------|------|
| **Scheduler service** (`cmd/server`) | gRPC server; in-memory registry of workers and tasks; queue + assignment on **`RequestTask`**; leases; failure detector; persistence hooks; Prometheus metrics. |
| **Worker agent** (`cmd/worker`) | Registers GPUs; polls **`RequestTask`**; runs **`DockerExecutor`**; **`ReportTaskStatus`**; **`Heartbeat`**; optional **`MonitorWorker`** stream for status sync. |
| **Submission client** (`cmd/submit`) | Minimal **`SubmitTask`** CLI. |
| **Persistence** | SQLite (default `scheduler.db`) or Postgres; migrates schema; load on startup + periodic save. |
| **Monitoring** | App stack: Prometheus + Grafana in `docker-compose.app.yml`. Separate **`docker-compose.monitoring.yml`** for Loki/Promtail/Alertmanager-style stack under `monitoring/`. |

**Execution flow**

1. Client **`SubmitTask`** → task stored as **`PENDING`**, `ExecutionSpec` JSON in `Task.configuration`, enqueued.  
2. Worker **`RequestTask`** → scheduler picks a schedulable task, sets **`RUNNING`**, assigns GPUs, **grants lease**, returns **`Task`**.  
3. Worker unmarshals spec, **`docker run`**, then **`ReportTaskStatus`** (**`COMPLETED`** / **`FAILED`**, etc.). Heartbeats refresh leases while **`RUNNING`**.  
4. Scheduler updates GPU/worker maps and **persists** snapshot on changes (when persistence enabled).

---

## 5. Task Lifecycle

| Transition | Meaning |
|------------|---------|
| **`PENDING` → `RUNNING`** | Successful **`RequestTask`** assignment. |
| **`RUNNING` → `COMPLETED` / `FAILED` / `CANCELED`** | Terminal states from **`ReportTaskStatus`** (and valid per `lifecycle.go`). |
| **`RUNNING` → `PENDING`** | **Lease expired** (reclaimed) or **worker offline**; task requeued, GPUs released. |
| **`RUNNING` → `PENDING` (retry)** | **`FAILED`** with retries remaining; backoff via **`NotBefore`**, then requeued. |

Terminal tasks (**`COMPLETED`**, **`FAILED`**, **`CANCELED`**) are not rescheduled from the scheduler’s transition rules.

---

## 6. How to Run

**Prerequisites:** Go **1.23+** (see `go.mod`), **protoc** with `protoc-gen-go` and `protoc-gen-go-grpc`, **Docker** on workers for execution.

**Build binaries**

```bash
make proto
make build
```

Binaries land in **`bin/`** (`server`, `worker`, `submit`, `monitor`).

**Start scheduler** (defaults: config `configs/scheduler.yaml` if present, port **50051**, SQLite `scheduler.db`)

```bash
go run ./cmd/server/main.go
```

Useful flags: `-port`, `-config`, `-metrics-port`, `-db-type=postgres`, `-db-connection=...`, `-tls-cert` / `-tls-key` / `-tls-ca`, `-auth-token` (see limitations).

**Start worker** (comma-separated GPU rows: id / model / memory MB; scheduler from **`SCHEDULER_ADDR`** or **`-scheduler`**)

```bash
go run ./cmd/worker/main.go -scheduler=localhost:50051 -gpus=0 -models=mock-gpu -memories=8192 -addr=localhost:0
```

If **`worker_id`** is omitted at registration, the scheduler assigns one (e.g. `worker-1`). Use that value with **`cmd/monitor`**.

**Submit job**

```bash
go run ./cmd/submit/main.go -scheduler=localhost:50051 -image=alpine:latest -command="echo hello" -gpus=1
```

**Monitor** (streams **`MonitorWorker`**; optional `-worker` ID)

```bash
go run ./cmd/monitor/main.go -scheduler=localhost:50051 -worker=<worker_id> -refresh=2
```

**Docker Compose (app + DB + metrics)**

```bash
docker compose -f docker-compose.app.yml up --build
```

Scheduler: `localhost:50051`. Prometheus: `http://localhost:9090`. Grafana: `http://localhost:3000` (see compose for admin defaults).

**Monitoring-only stack** (from repo root, see `monitoring/README.md`)

```bash
docker compose -f docker-compose.monitoring.yml up -d
```

---

## 7. Example Job

**CLI (works with `cmd/submit` today)** — `command` is split on whitespace into `ExecutionSpec.command`:

```bash
go run ./cmd/submit/main.go \
  -scheduler=localhost:50051 \
  -image=python:3.11-slim \
  -command="python -c \"print('hello')\"" \
  -gpus=1 \
  -priority=0
```

**Full `ExecutionSpec` (proto)** — as stored after submission; `docker run` receives `command` and `args` in order (`pkg/worker/docker_executor.go`):

```json
{
  "image": "alpine:3.19",
  "command": ["sh", "-c"],
  "args": ["echo hello && sleep 2"],
  "env": { "FOO": "bar" },
  "working_dir": "/tmp",
  "artifact_path": "/artifacts",
  "timeout_seconds": 300
}
```

Submitting that shape requires **gRPC** (or extending the CLI); the scheduler validates **`image`**, non-empty **`command`**, and **`required_gpus` > 0** on `SubmitTask`.

---

## 8. Design Choices

- **Pull-based scheduling:** Workers initiate **`RequestTask`**, which simplifies connectivity (workers dial out), avoids keeping per-worker push channels, and matches common batch-worker patterns.
- **Leases:** Short leases bound how long a dead worker can block reassignment; heartbeats tie lease renewal to liveness without constant polling for assignment.
- **Containers:** **`docker run`** isolates dependencies per job and matches the **`ExecutionSpec`** model; the tradeoff is an operational dependency on Docker and (today) no built-in GPU device wiring.

---

## 9. Future Work (Not Implemented)

- HA / sharded scheduler and/or external durable queue  
- Richer scheduling (preemption, gang scheduling, quotas)  
- Worker autoscaling  
- First-class cancel API and server-driven **`WorkerCommand`**  
- Web dashboard  
- GPU device injection (e.g. `--gpus`) aligned with assigned IDs  
- **`auth-token`** support in bundled CLIs  

---

## Repository layout (short)

- `proto/scheduler.proto` — API definitions (`ExecutionSpec`, `RequestTask`, …)  
- `cmd/server`, `cmd/worker`, `cmd/submit`, `cmd/monitor` — binaries  
- `pkg/scheduler` — core logic  
- `pkg/worker` — agent + Docker executor  
- `pkg/persistence` — DB layer  
- `configs/scheduler.yaml` — optional listen port, lease and heartbeat timeouts  
