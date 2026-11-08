# Opt-in localhost FastAPI and stable ngrok LaunchAgents

Prepared for `/Users/naufal/Moobby_back` on this Mac. Nothing here installs, loads,
or starts a service automatically. Two tracked templates are independent:

- `ops/launchd/dev.mooby.api.plist` supervises the existing Uvicorn app command.
- `ops/launchd/dev.mooby.ngrok.plist` supervises the existing ngrok HTTP command.

Both use `.venv/bin/python scripts/launchd_api.py` with an explicit service selector.
No API routes, database objects/permissions, worker/model rules or migrations change.
The existing shared flock utility from `scripts/worker_runtime.py` prevents duplicate
managed API or tunnel supervisors through separate `.local/services/*.lock` files.
It does not stop an older manually launched Uvicorn/ngrok process: stop those
manually before enabling either agent. A port/domain conflict otherwise causes a
sanitized unsuccessful exit, not an attempt to kill unrelated processes.

## Current blocker: no configured stable domain

During preparation, `/opt/homebrew/bin/ngrok` was found and the existing private
`~/Library/Application Support/ngrok/ngrok.yml` contained authentication but no
stable API endpoint/domain configuration. Other standard local ngrok config
locations did not provide one. No domain was invented or copied from an earlier
chat/session URL; account-side domain reservations were not queried.

The tunnel remains blocked until the owner identifies an already configured/reserved
stable domain for this Development API. The local config example intentionally has
`stable_domain: null`. If ngrok config already defines exactly one endpoint/tunnel
with a stable HTTPS URL/domain forwarding to localhost port 8000, the supervisor
can reuse it automatically. Unrelated upstreams (such as another app on port 3000)
are ignored. Multiple/absent domains require explicit owner configuration.

Alternatively, after confirming a stable domain in the existing ngrok account,
set its bare hostname in the ignored local JSON. Do not guess a hostname or use a
temporary/random URL. The code cannot prove the account owns/reserves a domain
without starting a tunnel or querying the account; ngrok enforces that when the
owner eventually starts it. This preparation neither performs those checks nor
modifies the ngrok config or account.

## Credentials and process/log boundaries

Copy `ops/launchd/development-api.json.example` to ignored
`ops/launchd/development-api.json`. It contains only:

- The independently reviewed expected Development Supabase project ref.
- Path to the existing private ngrok config file (no credentials copied).
- Optional already confirmed stable public hostname, otherwise null.
- Optional `mock_enabled` (boolean, default false) and `mock_ttl_minutes` (integer
  1–240, default 60), controlling only the dedicated mock endpoint.

Never put a database URL, bearer token, authtoken or API key in JSON/plist files.

For the API, local `.env` is parsed as data, not sourced by a shell, with expansion
and interpolation disabled. Only `APP_API_DATABASE_URL` and `APP_API_BEARER_TOKEN`
are selected and passed to Uvicorn as environment variables. Generic/admin/worker
URLs, inherited `PG*` routing, ngrok tokens and other `.env` settings are not used.
The database URL must use `mooby_app_api_login_dev` on
`db.<expected-ref>.supabase.co`, or `mooby_app_api_login_dev.<expected-ref>` on a
Supabase pooler hostname. The expected ref must be supplied independently; no code
can infer which project is Development from a naming convention alone.

Other API `.env` options, including synthetic-fixture flags and timeout overrides,
are intentionally not forwarded. Optional mock activation comes only from the
local JSON fields described below. The normal feed always reads the database;
existing default timeouts apply. This setup does not change `.env`, token values, or application behavior.
The bearer token must meet the API's existing minimum 32-character requirement.

The child commands reuse the current configuration/commands:

- `.venv/bin/python -m uvicorn mooby.api.app:app --host 127.0.0.1 --port 8000`,
  with access logging disabled and critical logging level.
- `ngrok http http://127.0.0.1:8000 --config <private-config-path> --url https://<stable-domain>`,
  with HTTP inspection and agent logging disabled.

Database credentials and bearer tokens are never command arguments. The ngrok
credential remains in its existing private config; it is not copied to environment,
arguments, tracked files, or supervisor logs. The tunnel child receives only a small
system-environment allowlist, **no API or database credentials**. The API child does
not receive ngrok credentials. No `--authtoken`/`--api-key` command is used.

Child stdout/stderr and launchd streams are `/dev/null`. Only fixed supervisor
lifecycle/configuration events and numeric child exit codes go to bounded rotating
logs (1 MB plus three backups). SQL/exception text, request URLs/headers, raw text,
evidence, author data and credentials are not logged. ngrok uses `--inspect=false`
and `--log=false`, so local HTTP inspection and configured agent log-file output
are disabled. See the [official ngrok CLI reference](https://ngrok.com/docs/gateway/agent/cli).
Account-side cloud traffic-export/inspection policies are outside this local setup
and are not inspected or modified here.

## Install configuration and files — no service starts yet

```bash
cd /Users/naufal/Moobby_back
cp ops/launchd/development-api.json.example ops/launchd/development-api.json
```

Edit the copied JSON locally. Replace the project-ref placeholder and confirm the
ngrok config path. Resolve the stable-domain blocker before installing/loading the
tunnel agent. Leave authentication in the existing private ngrok config, never in
the repo JSON. Keep the two API secrets in the existing private `.env`.

The following optional checks are local-only: no child, HTTP probe or DB connection.
They print only a controlled valid/blocked status and reason, never configuration
values. Config validation does not establish database, account or tunnel readiness.

```bash
.venv/bin/python -B scripts/launchd_api.py --service api --check-config
.venv/bin/python -B scripts/launchd_api.py --service tunnel --check-config
plutil -lint ops/launchd/dev.mooby.api.plist ops/launchd/dev.mooby.ngrok.plist
```

The tunnel check is expected to report `stable_domain_not_configured` until its
blocker is resolved (and API configuration is valid). Authentication presence in a
file does not prove that the token/account is valid; no remote validation occurs.

Install only the service files you intend to enable:

```bash
mkdir -p "$HOME/Library/LaunchAgents"
cp ops/launchd/dev.mooby.api.plist "$HOME/Library/LaunchAgents/dev.mooby.api.plist"
```

After the domain blocker is resolved, optionally install the tunnel file:

```bash
cp ops/launchd/dev.mooby.ngrok.plist "$HOME/Library/LaunchAgents/dev.mooby.ngrok.plist"
```

Copying is not loading now, but installed files can load at subsequent user login.
The repository templates alone do nothing. Do not install either until you intend
unattended Development operation. No sudo, permission changes, or LaunchDaemon is
needed. Existing files/agents must be stopped before replacement.

## Start — explicit owner action

Start the localhost API first:

```bash
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/dev.mooby.api.plist"
```

After independently confirming the local API works, and only when the stable domain
is configured, opt in to internet exposure:

```bash
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/dev.mooby.ngrok.plist"
```

There is no automatic startup dependency or HTTP readiness probe. At user login,
the two jobs may start in either order; ngrok may briefly return 502 until API
startup finishes. The tunnel does not replace bearer authentication. The API binds
only to `127.0.0.1`; no public network interface is opened by Uvicorn.

`RunAtLoad` starts the selected child on loading. Failed runtime exits are retried
with a 30-second throttle. Configuration blockers and duplicate managed instances
exit cleanly to avoid restart loops; fix the configuration and kickstart explicitly:

```bash
launchctl kickstart -k "gui/$(id -u)/dev.mooby.api"
launchctl kickstart -k "gui/$(id -u)/dev.mooby.ngrok"
```

Use the tunnel kickstart only after its blocker is resolved. Restart the API after
changing `.env`. Supervisors forward termination to the child process group and
use a bounded stop timeout. These are logged-in user agents; logout/sleep/network
loss can interrupt availability.

## Status and safe log review

```bash
launchctl print "gui/$(id -u)/dev.mooby.api"
launchctl print "gui/$(id -u)/dev.mooby.ngrok"
tail -n 50 /Users/naufal/Moobby_back/.local/services/api.log
tail -n 50 /Users/naufal/Moobby_back/.local/services/tunnel.log
```

`service_started` means a child was spawned, not that API/database readiness or
public tunnel establishment succeeded. `service_exited` records only its numeric
exit code. Detailed child errors are deliberately unavailable in these logs; use
the established controlled diagnostics separately instead of enabling raw logging.
A missing service after stop/uninstall is normal.

## Stop

Stop internet exposure first, then the API (only for jobs actually loaded):

```bash
launchctl bootout "gui/$(id -u)" "$HOME/Library/LaunchAgents/dev.mooby.ngrok.plist"
launchctl bootout "gui/$(id -u)" "$HOME/Library/LaunchAgents/dev.mooby.api.plist"
```

Leaving installed plist files permits startup at a later login. Uninstall them to
keep the services disabled across logins.

## Uninstall

After bootout, remove only the installed agent files:

```bash
rm "$HOME/Library/LaunchAgents/dev.mooby.ngrok.plist"
rm "$HOME/Library/LaunchAgents/dev.mooby.api.plist"
```

Keep `.env`, the private ngrok config, models, and database data. Ignored local JSON
and lifecycle logs may be retained for review. Never delete/replace an active lock.

## Preparation checks

```bash
PYTHONPATH=tests .venv/bin/python -B -m unittest test_api_launchd -v
plutil -lint ops/launchd/dev.mooby.api.plist ops/launchd/dev.mooby.ngrok.plist
```

Tests use temporary fake config/secrets and process doubles only. No database,
SQL, API, ngrok, worker, service installation/loading, migration or permission
operation was performed during preparation. Current blockers are the unconfigured
stable domain and the owner-supplied local project/config readiness; actual database
and ngrok account readiness remain unverified.

## Mock test pada port dan tunnel yang sama

The service now supports `/api/v1/disruptions` (always real database feed) and
`/api/v1/mock/disruptions` (Development-only fixtures) concurrently in one process,
on port 8000 with the existing ngrok domain. No worker or tunnel needs to stop.

In the existing ignored `ops/launchd/development-api.json`, add/update only:

```json
"mock_enabled": true,
"mock_ttl_minutes": 60
```

Keep the existing project ref, stable domain and ngrok config path. These are
members of the existing JSON object, not a replacement file. Mock is false when
omitted; TTL must be an integer in 1–240. The supervisor sets the three existing
Development fixture environment flags only when explicitly enabled. It also
forwards the existing `MOOBY_RELEVANCE_THRESHOLD`, `MOOBY_SEVERITY_THRESHOLD` and
`MOOBY_TRUSTED_PUBLISHED_AT_BASES` from `.env` for in-memory fixture processing,
without changing their values or forwarding worker database credentials. The
fixture processor retains its original behavior. Those flags
now gate the mock endpoint alone; they cannot redirect the normal endpoint.

When you choose to deploy changed code/config, restart only the already loaded API:

```bash
launchctl kickstart -k "gui/$(id -u)/dev.mooby.api"
```

Use bootstrap instead only if the job is unloaded, per the lifecycle section.
The API restart briefly interrupts HTTP handling, but the worker continues polling
and persisting normally. Subsequent normal and mock requests coexist; no continuing
service switch is needed. Do not bootout the normal API or launch a second Uvicorn
on port 8000.

Send bearer-authenticated requests to the mock path explicitly, for example:

```text
https://capsule-favored-eclair.ngrok-free.dev/api/v1/mock/disruptions?station_ids=open-krl:SRP&station_ids=open-krl:THB
```

Use the same token as the normal API, in the header only. Keep normal app requests
on `/api/v1/disruptions`; a separate mock UI/test selection must use the mock path.
This change does not automatically alter the iOS app. Expired/disabled mock returns
404, invalid configuration/fixture errors return 503, and mock never substitutes
for normal data. Disable via `mock_enabled: false` plus an API restart.

No service is restarted by this code change or its local tests.
