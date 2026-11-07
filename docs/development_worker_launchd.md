# Opt-in continuous Development worker on this Mac

The existing `scripts/run_worker.py` already continuously calls
`enqueue_recent_snapshots()` and `run_available()`. Each pass discovers committed
snapshots captured in the preceding hour, enqueues through the existing two-column
INSERT, and claims with the existing pipeline-filtered `FOR UPDATE SKIP LOCKED`
lease path. Idle passes sleep five seconds; available work is processed without
waiting for a full batch. This setup selects discovery batches of up to five and
**one processed item at a time**. It adds no second processing path.

A real worker run commits accepted results. It is distinct from the rollback-only
smoke harness. The unchanged intake/NLP/location/evidence/embedding gates and
publication rules apply. Old publications outside the intake window are still
skipped. No synthetic fixture or simulated-time settings are forwarded. Recovery
uses the existing queue lease/retry policy, not a new retry processor.

## Files and safeguards

- `ops/launchd/dev.mooby.worker.plist`: unloaded LaunchAgent template for this repo
  at `/Users/naufal/Moobby_back`, label `dev.mooby.worker`.
- `scripts/launchd_worker.py`: configuration/logging/process supervisor only. It
  launches the existing CLI with the repo's `.venv/bin/python`.
- `ops/launchd/development-worker.json.example`: non-secret config template. Copy
  it to ignored `development-worker.json` and provide the reviewed Development
  project ref, existing stable pipeline version, and this Mac's stable worker ID.
- `scripts/worker_runtime.py`: URL validation, a shared `flock` lock, and the log
  allowlist. Every invocation of the existing CLI acquires the same repo-local
  `.local/worker/instance.lock` before connecting. Competing CLI invocations exit
  cleanly without connecting. Never remove/replace a live lock file.

The expected ref must be supplied independently from the reviewed **Development**
project identity. The code validates an exact Supabase ref; it cannot establish
which project is Development merely from a hostname. Direct URLs must use
`db.<expected-ref>.supabase.co` with `mooby_nlp_worker_login_dev`. Pooler URLs must
use a `.pooler.supabase.com` host with username
`mooby_nlp_worker_login_dev.<expected-ref>`. Admin/owner/gateway usernames and
query-string host/user/service/options overrides are rejected. The CLI also applies
this guard for direct manual invocations.

The supervisor reads local `.env` with `python-dotenv` as data, with interpolation
disabled; it never sources shell commands or prints values. The only database URL
used is `WORKER_DATABASE_URL` **from that file**, never `DATABASE_URL`, an admin URL,
or an inherited connection URL. It forwards only that URL, the existing three
worker threshold/provenance settings, optional `MOOBY_EMBEDDING_MODEL_DIR`, and a
small system-environment allowlist. Inherited `PG*`, `PYTHONPATH`, API credentials,
and other `.env` values are not passed to the child. Existing worker settings
validation is reused without changing any value or rule.

Both model families and their runtime dependencies must already be installed
locally. Hugging Face/Transformers are forced offline. The agent does not download
artifacts, install dependencies, apply migrations, grant permissions, start the
API/ngrok, or change collection data. Schema/worker grants through migration 017
must already be available; 017 records the narrow conflict-target SELECT grants
reported as manually applied in Development. This preparation does not establish
actual database readiness.

## Install files and validate — no service starts in these steps

Stop any previously running manual worker before enabling the agent. Processes
started before this lock implementation cannot be retroactively locked. Use this
single LaunchAgent label for this checkout; do not create another processing agent.

From the repo, copy the local config template:

```bash
cd /Users/naufal/Moobby_back
cp ops/launchd/development-worker.json.example ops/launchd/development-worker.json
```

Edit the copied JSON locally. Replace both `REPLACE_...` placeholders. Reuse the
pipeline version already selected for this Development deployment; changing it can
reprocess snapshots under a new namespace. Keep it stable across restarts, and
change it only as part of an intentional rules/model deployment. Keep worker ID
`mooby-dev-worker-naufal-mac` stable for this Mac (or choose another stable unique
ID). Never put connection URLs or credentials in the JSON or plist.

Ensure `.env` already contains the dedicated worker URL and valid existing
`MOOBY_RELEVANCE_THRESHOLD`, `MOOBY_SEVERITY_THRESHOLD`, and
`MOOBY_TRUSTED_PUBLISHED_AT_BASES`. Use literal values; shell expansion and dotenv
variable interpolation are intentionally disabled. No URL or threshold is supplied
or changed by this setup.

Run the optional local-only checks manually:

```bash
.venv/bin/python -B scripts/launchd_worker.py --check-config
HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 .venv/bin/python -B scripts/check_embedding_runtime.py
plutil -lint ops/launchd/dev.mooby.worker.plist
```

`--check-config` checks file/target/worker settings and returns only a safe status.
It neither connects nor starts a child. The embedding check performs local model
inference only. Check that classifier artifacts from the existing private baseline
bundle are also present. Model readiness, grants and connectivity must be confirmed
separately through the established Development procedure before enabling polling.

Copy the plist, without loading it:

```bash
mkdir -p "$HOME/Library/LaunchAgents"
cp ops/launchd/dev.mooby.worker.plist "$HOME/Library/LaunchAgents/dev.mooby.worker.plist"
```

Installing this file is opt-in and causes loading at subsequent user login. Do not
copy it there until you intend unattended Development operation. The tracked plist
alone does nothing. No `sudo`, chmod, permission grants, or LaunchDaemon is needed.
Paths in the plist are specific to this Mac/repo; do not install it for another clone.

## Start — explicit action, enables real Development writes

Only after configuration/model/schema readiness has been confirmed:

```bash
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/dev.mooby.worker.plist"
```

`RunAtLoad` starts the supervisor immediately. It remains a logged-in user agent,
not a system daemon; macOS sleep/logout/offline periods pause or stop useful work.
The one-hour discovery/intake windows are unchanged, so outages can leave older
posts outside normal intake. No attempt is made to prevent sleep or bypass gates.

Runtime failures cause an unsuccessful exit; launchd restarts with a 30-second
throttle. Bad local configuration exits cleanly after a sanitized
`configuration_invalid` record to avoid a retry loop; fix it and explicitly
kickstart. A duplicate-instance exit is also clean. To reload changed `.env`/JSON
or model configuration while the agent is installed and loaded:

```bash
launchctl kickstart -k "gui/$(id -u)/dev.mooby.worker"
```

SIGTERM/interrupt is forwarded to the child process group, with a bounded shutdown
wait. Interrupted queue leases recover through the existing expiry policy.

## Stop and status

Stop and unload the current job:

```bash
launchctl bootout "gui/$(id -u)" "$HOME/Library/LaunchAgents/dev.mooby.worker.plist"
```

Check status without starting anything:

```bash
launchctl print "gui/$(id -u)/dev.mooby.worker"
```

A missing service after bootout is expected. A loaded service is not proof that
processing is healthy; inspect its PID/exit state and safe worker logs. A plist
left in `~/Library/LaunchAgents` can load at the next login; uninstall it if you
want it to remain disabled across logins.

## Log review

```bash
tail -n 50 /Users/naufal/Moobby_back/.local/worker/worker.log
```

The supervisor writes bounded rotating logs (1 MB plus three backups). It parses
CLI stdout as JSON, keeps only known lifecycle events/numeric poll counters and
SQLSTATE, and drops all other fields/lines. Child stderr and launchd stdout/stderr
are `/dev/null`; raw library output, tracebacks, SQL, source text, quotes, authors,
IDs, credentials and exception messages never get copied into these logs. Worker
CLI errors themselves emit only a generic event and optional SQLSTATE. Event
`worker_started` means the CLI reached startup, not that its database is ready.

Poll counters show enqueued/processed/succeeded/skipped/rejected/failed counts and
accepted-event count, not source content or reason-code text. A succeeded
classification without a draft is still not a published event. `worker_error`
with SQLSTATE and `worker_exited` are enough to identify a failure requiring the
existing controlled diagnostics; do not enable raw DB-error logging. Configuration
or missing-runtime failures may expose only a generic event or exit code.

Logs/lock live under ignored `.local/worker/`; the local JSON is ignored. Keep
`.env` private through the project's existing secret-handling process. No permissions
are changed by the prepared setup or its validation tests.

## Uninstall

First stop/bootout using the command above if loaded. Then remove only the installed
agent file:

```bash
rm "$HOME/Library/LaunchAgents/dev.mooby.worker.plist"
```

Keep `.env`, models and database data. Local JSON/logs may be retained for review.
Do not delete the lock file while any manual or managed worker is running.

## Local verification performed during preparation

```bash
PYTHONPATH=tests .venv/bin/python -B -m unittest test_worker_launchd -v
plutil -lint ops/launchd/dev.mooby.worker.plist
```

Tests use temporary files, fake connections/processes and mocked polling helpers.
They do not connect to any database or start a worker/API/ngrok. This setup has not
been installed, loaded, kickstarted, or exercised against Development.
