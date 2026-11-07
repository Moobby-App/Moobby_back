# Required worker embedding: manual Development operations

The worker accepts only `succeeded` processor results with a valid draft. It then
embeds the existing normalized derived post text using `passage: `, L2 normalization,
and the locked `intfloat/multilingual-e5-small` model. No classification thresholds
change. The vector must have exactly 384 finite components and unit L2 norm.
There is no random/zero fallback and no human review or correlation requirement.
The text and exact evidence stay private; neither vector nor model/run metadata
is exposed by the gateway. `verified` is machine acceptance, not official confirmation.

The event vector and input version live in `nlp.event_versions`; the linked
`nlp.processing_runs` holds embedding model version/digest. Successful embedding
is prepared before the finish transaction. Queue completion, run, event, vector,
source provenance, and evidence commit or roll back together. Inference/load or
contract failure writes only an error run/note and retry state, with reason
`transient_embedding_error`. Existing retry limits apply; an exhausted attempt fails.
No previous draft is promoted. Conflicts retain the existing event version.

## Manual Development migration

Migration 015 is tracked in Git. Git cannot establish whether a database has
applied it. Its optional-embedding behavior is superseded; do not deploy this
worker with nullable embedding columns or use 015 alone as a workaround.
Migrations 001–015 were not edited by this change.

The owner must inspect their migration ledger and current schema in the intended
**Development** project before applying `016_required_event_embedding_publication.sql`
through their established migration process. It works after 014 or after 015:

- Reasserts `embedding` and `embedding_input_version` as NOT NULL.
- Adds only column-level INSERT for these two fields plus verification/publication
  statuses and their timestamps, to the existing worker role.
- Does not change role membership, RLS, ownership, collection, or old event data.

If historical rows contain NULL embeddings/input versions, SET NOT NULL fails and
the whole migration rolls back, including grants. Stop and plan historical-data
handling separately. Do not fabricate vectors or mass-promote old drafts. This
change does not apply migrations or inspect/mutate database contents.

### Worker conflict-target SELECT grants

Migration `017_worker_conflict_target_select.sql` records three narrow column-level
SELECT grants required by the worker's explicit `ON CONFLICT` targets:

| Table | SELECT columns | Conflict target |
|---|---|---|
| `nlp.events` | `event_id` | `(event_id)` |
| `nlp.event_sources` | `event_id`, `event_version`, `source_snapshot_id` | `(event_id, event_version, source_snapshot_id)` |
| `nlp.event_evidence` | `evidence_id` | `(evidence_id)` |

These grants have already been applied manually in Development, as reported by
the project owner. Migration 017 records that state for reproducible deployments;
this code change did not connect to or independently inspect Development. Repeating
these GRANTs does not broaden their scope. Follow the existing migration process
and reconcile its ledger with the manual application before recording deployment.

The sole grantee is `mooby_nlp_worker_dev`; transaction-local owner-role handling
matches prior migrations. No table-wide SELECT, raw text/evidence access, API view
access, membership, RLS, ownership, collection object, or other permission changes
are included. These columns do not provide general event/audit inspection access;
the rollback smoke's separate post-rollback residue inspector remains necessary.

## Providing the real model locally

The optional runtime is separate from the API and unit-test dependencies. From
the repository root, the owner can install it manually:

```bash
.venv/bin/python -m pip install -e '.[embedding]'
```

Use the reviewed model bundle from the previous host, or download the exact locked
revision deliberately. This download is not performed by the worker:

```bash
.venv/bin/python - <<'PY'
import json
from huggingface_hub import snapshot_download
lock = json.load(open('evaluation/embedding_model.lock.json'))
snapshot_download(lock['model_id'], revision=lock['revision'])
PY
```

The worker loads the locked snapshot from the local Hugging Face cache only, with
remote code disabled and CPU inference. Alternatively set `MOOBY_EMBEDDING_MODEL_DIR`
to a trusted local copy of the same complete artifact tree. Successful loads are
cached per process. Restart the worker after changing its artifact configuration.

## Audited manifest and digest contract

The model ID `intfloat/multilingual-e5-small` and revision
`614241f622f53c4eeff9890bdc4f31cfecc418b3` are unchanged. The cached snapshot was
audited locally: exactly 43 snapshot entries, all symlinks to the matching model
cache's `blobs` directory. For 33 ordinary Git blobs, Git SHA1 of
`b"blob " + decimal_byte_length + b"\0" + content` matched each cache key
(the separator is a single NUL byte). For 10 LFS blobs, SHA256(content) matched
the cache key. This establishes local cache content-address consistency and the
pinned snapshot layout, not remote-signature authentication or a fresh Hub listing.
No remote lookup or model download was used for this audit.

`evaluation/embedding_model.manifest.json` records:

```json
{
  "format": "model-bundle-manifest-v1",
  "model_id": "intfloat/multilingual-e5-small",
  "revision": "614241f622f53c4eeff9890bdc4f31cfecc418b3",
  "files": [{"path": "relative/POSIX/path", "sha256": "lowercase SHA256 of file bytes"}]
}
```

The actual manifest contains all 43 entries, not this illustrative placeholder.
All regular files are included recursively, including dotfiles and `.cache` files
if present; **there are no exclusions**. File symlinks are dereferenced and their
target bytes are hashed; link text/absolute target paths are not recorded. A copied
bundle with regular files instead of symlinks is equivalent if bytes and relative
names match. Broken symlinks, directory symlinks and special files are rejected.
Directories themselves, including empty directories, are not manifest entries.

Entries are sorted lexicographically by relative POSIX path (Python Unicode string
order); paths must be canonical, relative, unique, without `..`, backslash, or NUL.
Manifest keys are exactly `format`, `model_id`, `revision`, `files`; entry keys are
exactly `path` and `sha256`. The digest algorithm is
`sha256-model-bundle-manifest-json-v1`:

1. Validate manifest structure and ordering.
2. Serialize with Python `json.dumps(manifest, ensure_ascii=False, sort_keys=True,
   separators=(',', ':'), allow_nan=False)` and encode UTF-8, without a BOM.
3. SHA256 of ASCII `sha256-model-bundle-manifest-json-v1`, one LF byte,
   the canonical JSON bytes, and one final LF byte.

Pretty-printing the checked-in JSON does not affect its canonical digest. The lock
binds the algorithm, manifest filename, model/revision, file count and derived digest.
The provider checks that binding and recomputes the full file set and every content
hash before loading a model. Added, missing, renamed or changed files fail closed.
Validation occurs on model load; a successfully loaded model remains cached until
process restart, as before.

The historical algorithm label was `sha256-sorted-path-and-file-sha256-v1`, with
lock digest `c43093c0c63bdc8984e3ffba0e85b6ce4b8d4c0ed284cd0f95180e3a890f2802`.
Its producer serialization was absent from the repository. The previous provider
instead produced `ce21f1396ac116e38fc20b9b531780021cfbb0d6ccc032385c05035ff203b07f`.
Neither value is accepted by the new algorithm. The independently derived manifest
digest is `35acb78bc7ecf1eeb5650f83d1d839365010be4a2b96fd70da686bebe8ec88d9`.
Model bytes, model revision, and embedding input/inference behavior are unchanged.

Repeat verification using the existing cache only (no model loading):

```bash
HF_HUB_OFFLINE=1 .venv/bin/python scripts/verify_embedding_bundle.py
```

To independently reproduce the manifest and repeat the content-addressed cache audit
without changing any files, print it to stdout:

```bash
HF_HUB_OFFLINE=1 .venv/bin/python scripts/verify_embedding_bundle.py --audit-cache
```

Compare that JSON with the tracked manifest. This tool does not update the lock,
write a manifest, or download anything. For any mismatch, stop and investigate;
never regenerate the lock just to accept unexplained file changes. Manifests/locks
belong in Git, model binaries and absolute cache paths do not.

Focused local tests (temporary files; no cache mutation or database):

```bash
PYTHONPATH=tests .venv/bin/python -m unittest test_bundle_manifest test_embedding_pipeline.EmbeddingTests -v
```

Run the offline smoke check before starting a worker:

```bash
.venv/bin/python scripts/check_embedding_runtime.py
```

It prints only status, dimension and provenance, never a vector or source text.
A nonzero exit means runtime/model/contract is unavailable; do not run the worker
expecting publication. This repository's unit tests use explicit test doubles;
they do not establish that real weights load or that inference succeeds locally.

Provider conventions follow the [E5 model card](https://huggingface.co/intfloat/multilingual-e5-small)
and [SentenceTransformer API](https://www.sbert.net/docs/package_reference/sentence_transformer/SentenceTransformer.html).

## Read-only verification after owner deployment

Run these manually using an authorized administrative/read-only inspection session
in Development. The gateway role intentionally cannot inspect private NLP tables.
These queries expose no source text, evidence, author, or private snapshot IDs.
They were not executed during implementation.

```sql
-- Both columns must be NOT NULL, and embedding must remain vector(384).
SELECT a.attname, a.attnotnull,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type
FROM pg_catalog.pg_attribute AS a
WHERE a.attrelid = 'nlp.event_versions'::regclass
  AND a.attname IN ('embedding', 'embedding_input_version')
  AND NOT a.attisdropped;

-- Preflight: any nulls block migration 016; do not fabricate replacements.
SELECT count(*) FILTER (WHERE embedding IS NULL) AS missing_vectors,
       count(*) FILTER (WHERE embedding_input_version IS NULL) AS missing_input_versions
FROM nlp.event_versions;

-- Effective minimum INSERT permissions should all be true after migration 016.
SELECT column_name,
       has_column_privilege('mooby_nlp_worker_dev', 'nlp.event_versions',
                            column_name, 'INSERT') AS can_insert
FROM unnest(ARRAY['embedding', 'embedding_input_version', 'verification_status',
                  'verified_at', 'publication_status', 'published_at']) AS t(column_name);

-- Verify newly produced rows only; no updates or promotion.
SELECT ev.event_id, ev.event_version, ev.verification_status, ev.verified_at,
       ev.publication_status, ev.published_at,
       extensions.vector_dims(ev.embedding) AS embedding_dimension,
       ev.embedding_input_version,
       r.embedding_model_version, r.embedding_model_digest
FROM nlp.event_versions AS ev
JOIN nlp.processing_runs AS r ON r.run_id = ev.processing_run_id
WHERE ev.created_at >= statement_timestamp() - interval '1 hour'
ORDER BY ev.created_at DESC
LIMIT 20;

-- Existing gateway projection still filters latest published versions and 24h.
SELECT public_event_id, station_id, affected_station_ids, freshness_anchor_at
FROM app_api.published_disruptions
ORDER BY freshness_anchor_at DESC
LIMIT 20;
```

A published row can be absent from the last query if its location/time falls outside
the existing projection rules. Null/unknown event time is allowed; existing fallback
freshness policy remains unchanged. Synthetic fixture mode remains an isolated,
in-memory UI simulation and does not insert/publish normal database events.

## One historical snapshot, rollback-only Development smoke

See [Development smoke harness](development_smoke.md) for one-item real-model
processing via the existing enqueue/claim helpers, unconditional worker rollback,
and post-rollback read-only NLP residue inspection. API contract tests are separate. It is manual-only and was not run against a
database during implementation.
