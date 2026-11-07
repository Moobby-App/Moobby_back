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

Artifact validation compares the measured content digest with the existing lock.
Serialization is explicitly: iterate sorted relative POSIX file paths, excluding
`.cache` directory entries; concatenate each UTF-8 path, NUL byte, lowercase file
SHA256 hex, newline; SHA256 the resulting bytes. This includes weights, tokenizer,
configuration, and other bundle files. The legacy lock names the digest method but
has no accompanying producer implementation. If the original bundle used another
serialization or differs in file inventory, validation fails closed. Reconcile the
trusted bundle/lock provenance separately; do not overwrite the expected hash merely
to bypass a mismatch. No lock hash or revision is changed in this implementation.

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
