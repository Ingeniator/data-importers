# Feature: Scheduled Auto-Import

## One-liner
Let admins define recurring import pipelines (source → filter → dataset service) that run automatically on a cron schedule, without manual browser interaction.

## Problem (from docs/tasks-feature-questions.md)
Evaluation datasets need to be kept fresh — new traces from production should flow into the dataset service daily/hourly without someone manually clicking "Import" in the UI. Today there is no automation layer.

## Design decisions already made
- **Scheduler location**: In-process APScheduler inside the FastAPI app. Simpler than K8s CronJobs, works in docker-compose local dev. State in SQLite (`scheduled_tasks.db`). Acceptable for v1; K8s CronJob migration is a separate concern.
- **Task definition**: Admin-defined in `config.yaml` alongside datasources (no dynamic storage needed for v1).
- **Pipeline**: `source (datasource + filter params)` → `sampling rules (optional)` → `destination (target)`.
- **Execution**: Enqueues an arq job (if Redis configured) or runs inline — same import pipeline as manual imports.
- **Filter types supported**: field equality, relative time window ("last N hours"), full query string.

## Config schema sketch
```yaml
scheduled_tasks:
  - name: "Daily production sample"
    cron: "0 6 * * *"          # 06:00 UTC daily
    datasource: "ClickHouse"
    filter:
      time_window: "24h"       # relative to run time
      query: "error"           # optional text filter
    sampling:
      - strategy: random
        rate: 10
    target: "Production Dataset Service"
    dataset_name: "daily-prod-{date}"
```

## Scope
- **In**: APScheduler in FastAPI process; task config in `config.yaml`; `GET /api/public/scheduled-tasks` list endpoint; last-run status + next-run time per task; structured log + Prometheus counter per execution.
- **Out**: Runtime task creation/editing via UI (v2); K8s CronJob backend; per-task error notifications (Slack/email).

## Open questions
- Should scheduled task results create a new dataset per run, or append to an existing named dataset?
- How many concurrent scheduled tasks should be allowed? (Shares the `max_jobs=1` worker constraint.)
