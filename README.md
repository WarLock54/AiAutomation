# AI Automation — Autonomous GitFlow, Test Generation & Publishing Engine

*[Türkçe](README.md)*

This project is an **autonomous DevOps engine** that runs on top of a Go
microservices architecture (`order-project/order-engine/` — a separate
Saga/idempotency/circuit-breaker portfolio project, documented in its own
[README](order-project/order-engine/README.md)). Its goal: on every
`push`, without any human intervention, analyze code changes, detect
missing tests and scaffold them, record findings into a persistent task
list, and automatically publish passing code as Docker images.

This isn't a paper design — every component was run repeatedly against a
real GitHub Actions environment, and **real bugs were found and fixed**
along the way (see "Bugs Found and Fixed in Production" below).

## Architecture — 4 Pillars

```
git push
   │
   ▼
┌─────────────────────────┐
│ 1. GitFlow & Commit      │  ai_devops/gitflow_agent.py
│    Agent                 │  Parses `git diff --name-status` to
│                          │  STRUCTURALLY derive which files/services/
│                          │  tests/migrations/Dockerfiles/dependencies
│                          │  changed
└───────────┬──────────────┘
            │ ChangeAnalysis
            ▼
┌─────────────────────────┐
│ 2. Task Generator         │  ai_devops/task_generator.py
│    (.txt Reporting)      │  Writes findings to ai-improvement-tasks.txt
│                          │  with severity+ID+created/last_seen,
│                          │  PERSISTENTLY (deduped; never overwritten
│                          │  wholesale on each run)
└───────────┬──────────────┘
            │
            ▼
┌─────────────────────────┐
│ 3. Autonomous Testing     │  ai_devops/testing_engine.py
│    Engine                │  For EVERY Go package left without tests,
│                          │  generates a testify-based skeleton
│                          │  DELIBERATELY marked t.Skip (never a
│                          │  fake-green test)
└───────────┬──────────────┘
            │
            ▼
┌─────────────────────────┐
│ 4. CI/CD & Publish        │  .github/workflows/ai-pipeline.yml
│    Pipeline               │  gofmt → go vet → gitleaks → govulncheck →
│                          │  go test -race → Docker build+push (GHCR) →
│                          │  bot pushes its own commit
└──────────────────────────┘
```

`ai_devops_engine.py` is the orchestrator that runs the three Python
components (1-3) in sequence; the 4th pillar is defined entirely inside
`.github/workflows/ai-pipeline.yml`.

## Pillar Details

### 1. GitFlow & Commit Agent (`ai_devops/gitflow_agent.py`)

Change analysis used to be a crude substring search ("does the diff text
contain the string `order-service`?"). Now the output of
`git diff --name-status` is parsed line by line into a structured
`ChangeAnalysis` object: which services were affected, how many `.go`
files changed, how many of those are test files, whether any
migration/Dockerfile/`go.mod` changed. First-commit and shallow-clone
scenarios are also handled safely (by diffing against the fixed empty-tree
SHA that exists in every git repository).

### 2. Task Generator (`ai_devops/task_generator.py`)

Generates tasks from the `ChangeAnalysis` and writes them to a
**persistent** file (`ai-improvement-tasks.txt`). Every task carries a
deterministic ID (a short SHA-1 hash of its description); when the same
finding is detected again, no new line is added — only the `last_seen`
timestamp is updated. A human closes a task simply by deleting its line.

### 3. Autonomous Testing Engine (`ai_devops/testing_engine.py`)

Scans changed `.go` files; for any package with **zero** `_test.go`
files, it generates a testify-based skeleton file
(`<file>_ai_test.go`) targeting that package's exported functions. It
NEVER touches a package that already has at least one test file (human
tests are never overwritten). Every generated test function is
deliberately marked `t.Skip` — static analysis can't know a function's
actual behavior, so the generated skeleton never creates the illusion
that "green means verified."

### 4. CI/CD & Publish Pipeline (`.github/workflows/ai-pipeline.yml`)

In order: `gofmt` → `go vet` → `gitleaks` (secret scanning) →
`govulncheck` (known-vulnerability scanning, informational) →
`go test -race` (with coverage, threshold informational) → **build and
push Docker images for all 4 services to GHCR (`ghcr.io`)** → the bot
commits and pushes its own generated task report / test skeletons.

## Proven End-to-End

The screenshot below is from the run where the **entire** pipeline
(secret scan through the Docker build+push of all 4 services) completed
successfully end-to-end — a GHCR package was produced for each of
`order-service`, `inventory-service`, `payment-service`, and
`notification-consumer`:

![AI Pipeline — successful run: secret scan clean, Docker build+push completed for all 4 services](docs/ci-pipeline-success.png)

## Bugs Found and Fixed in Production

This engine was hardened by running it repeatedly against a real GitHub
Actions environment. Not just theoretical — these are issues actually
**encountered and fixed** in practice:

| Issue | Root cause | Fix |
|---|---|---|
| "0 files changed" on the first commit | `git diff --root HEAD` was used incorrectly (`--root` is for `git log`; plain `git diff` with a single ref compares the commit to the working directory) | Switched to diffing against the fixed empty-tree SHA (`4b825dc6...`) that exists in every git repository |
| Script crashing when there are no commits at all | If even `HEAD` doesn't resolve (e.g. `git init` was run but no commit made yet), a raw git error was raised | Added a dedicated `_has_any_commit()` check; the situation is now reported clearly and the script exits gracefully |
| Script crashing on Windows (`UnicodeDecodeError`) | `subprocess.run(text=True)` used the OS's default encoding (cp1254 on Turkish Windows); git's output is UTF-8 | `encoding="utf-8", errors="replace"` is now explicitly set on every `subprocess.run` call |
| Generated `.go` files failing `gofmt` on Windows | Python's text mode (`open(..., "w")`) automatically converts `\n` to `\r\n` on Windows | File writes now explicitly pass `newline="\n"` |
| gofmt failure in a generated test file | The template joined function blocks with an extra blank line | Fixed the join logic (`"\n\n".join`) |
| `order-project` was added as a git submodule/gitlink | A forgotten, embedded `.git` folder existed inside it | The embedded `.git` was removed and the files were re-added as normal content |
| `gofmt` failing in CI (on pre-existing code) | Original files had CRLF line endings, plus one genuine alignment bug | Normalized the whole tree with `gofmt -w .`, added `.gitattributes` to prevent recurrence |
| `govulncheck` failing on every push with new findings | Its vulnerability database is live and constantly updated; a hard gate turned it into a moving target instead of a stable green CI | The scan still runs (visibility preserved) but no longer blocks the pipeline (`continue-on-error`); the fixes that were actually available (upgrading to Go 1.24, `go-redis` v9.6.3) were applied |

## Setup & Usage

```bash
python3 ai_devops_engine.py
```

Run from the repo root, this triggers GitFlow Agent → Task Generator →
Autonomous Testing Engine in sequence. The real usage pattern, however,
is automatic on every `push` via `.github/workflows/ai-pipeline.yml`;
running it locally is only for development/debugging.

## Out of Scope

This system operates at the level of **static analysis + CI automation**.
The following were deliberately left out of scope (each would require a
separate, substantially larger project):

- **Chaos/load-test injection** (k6/Locust combined with Chaos Mesh or
  docker-compose-based outage simulation)
- **Feeding runtime observability data (Jaeger/OTel) into an AI model for
  interpretation** (e.g. detecting "the saga is deadlocking here")
- **Generating tests from an observed runtime failure** (the current
  engine only performs static code analysis — "this package has no
  tests" — it does not interpret an actual failure observed during a
  chaos scenario and generate a test targeted at it)
- **AI-driven analysis of Docker/cloud resource usage (CPU/Memory) to
  propose Terraform/compose patches**

## Related Sub-Project

`order-project/order-engine/` is the actual Go microservices system this
autonomous pipeline runs on top of (Saga orchestration, idempotency,
transactional outbox, circuit breaker, distributed tracing). See its own
architecture, proven guarantees, and setup instructions in
[order-project/order-engine/README.md](order-project/order-engine/README.md).
