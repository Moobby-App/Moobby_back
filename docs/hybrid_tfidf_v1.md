# Final KRL Hybrid TF-IDF v1.0 integration

## Active implementation

`src/mooby/worker/classifier.py` loads ONLY the packaged final `stage1.joblib` and
`stage2.joblib`. `model_config.py` selects the module-relative
`artifacts/krl_hybrid_final_v1/` directory. A hash manifest checks both stages and
metadata before deserialization; corrupt/missing/incompatible artifacts fail closed.
No fallback model, calibration, training or altered hyperparameter is introduced.

The supplied ZIP and extracted reference files are preserved. Deployment copies
contain stage1/stage2, metadata, requirements and the hash manifest, not research
data, predictor bytecode or notebooks. The original probability adapter is archived
as `legacy_classifier.py`; no production entry point imports it.

## Inference and contracts

- `normalize_model_text(text)` is exactly `re.sub(r"\s+", " ", text).strip()`.
  Input must be a string; empty normalized input is rejected before inference.
- Stage 1: word TF-IDF (1,2) + LinearSVC, integer 0/1.
- Stage 2: supplied word TF-IDF (1,2) + character **char_wb** TF-IDF (3,5) union,
  LinearSVC. Run only after disruption; its labels are light/moderate/heavy.
- `ClassifierAdapter.predict(raw_text)` matches supplied `predictor.py` output:
  `{"prediction":"heavy","is_disruption":true}`, or not_disruption/light/moderate.
- `predict_many(list_of_texts)` preserves input order, validates the complete batch,
  and applies independent inference to each tweet. No event grouping/history required.
- No probability or confidence is available. Internal prediction metadata records
  both as JSON null. Legacy probability thresholds are not applied to this model.
- Existing DB/public response severity remains light/moderate/**severe**. The adapter
  maps heavy → severe for storage, preserving `model_label=heavy` in private processing
  metadata. This avoids a database enum migration or frontend response-contract change.

The worker retains its existing keyword, station/path, trusted timestamp and evidence
gates. Their NFKC/casefold matching copy is kept separate from model input: both
classifier stages receive the original raw source text and apply whitespace only.
No prediction requires event clustering. Existing publication/location gates may
still skip a correctly classified tweet, as before.

The worker initializes `_process_runtime()` once at startup before polling and
fails closed if artifacts cannot load. The API mock fixture cache loads the adapter
once on first mock use. Neither stage reloads during inference.
Worker and API mock use the same configured final artifact directory.

## API and DB integration

There was no public raw-tweet classification HTTP endpoint, and none is added.
Active inference remains in the queue worker and synthetic fixture processing:

`source snapshot → existing gates → final classifier → evidence/location checks → existing atomic event persistence`.

Existing `/api/v1/disruptions`, `/api/v1/mock/disruptions`, auth and readiness routes
remain unchanged. Published records continue through the existing projection, expiry
policy, Redis cache and API response models. Stored historical results are not rewritten
or automatically re-enqueued. APNs development remains paused/unmodified.

## Dependencies and portability

Artifact requirements are pinned in `pyproject.toml`: scikit-learn 1.9.1, joblib 1.6.0,
NumPy 2.5.5, SciPy 1.18.1. Only local NumPy changed from 2.5.4 to 2.5.5; no other
backend dependency was replaced. `pip check` passes.

Training metadata reports Python **3.14.3**, while this backend deliberately retains
its existing Python **3.12.16** runtime (`>=3.12,<3.13`). Both stages load and run in
that runtime, with identical reference predictions in tested samples. This is empirical
compatibility verification, not a guarantee of arbitrary pickle portability across
Python/library versions. Future runtimes must repeat the tests. Installed-wheel testing
also verifies module-relative paths work outside the checkout.

## Startup and offline inference

Install with the repository's existing virtual environment/package workflow:

```sh
cd /Users/naufal/Moobby_back
.venv/bin/python -m pip install -e .
launchctl kickstart -k "gui/$(id -u)/dev.mooby.worker"
launchctl kickstart -k "gui/$(id -u)/dev.mooby.api"
```

Existing `.env` probability thresholds may remain for archived tools; they are ignored
for final Hybrid classification and are no longer mandatory in the worker supervisor.
Trusted timestamp provenance settings, database credentials and other controls remain
mandatory. Do not change the queue pipeline version to force historical reprocessing.

Offline inference (no DB, API server or embedding download):

```sh
printf '%s' '"KRL di Stasiun Serpong terlambat 10 menit karena antre masuk stasiun."' | .venv/bin/python -B scripts/predict_krl.py
printf '%s' '["KRL lancar hari ini", "Gangguan perjalanan KRL 😭"]' | .venv/bin/python -B scripts/predict_krl.py
```

No arbitrary model-upload endpoint or caller-controlled artifact URL is provided.
Only deserialize model files supplied by a trusted deployment administrator.

## Verification and research limitations

`tests/test_hybrid_classifier.py` covers reference equivalence, raw normalization,
empty/invalid input, Unicode/emoji, ordered batch, stage-2 short-circuit, load-once,
checksum corruption, version mismatch and real-model → worker → fake transactional
storage. No production database writes or training are used by these tests.
All model bytes are identical to the ZIP. A wheel built in a temporary directory
contains both stages and loads successfully after relocation without the research
directory. Python bytecode and datasets are excluded.

Metadata's macro-F1 0.7564 / heavy-F1 0.6055 are historical exploratory OOF scores,
not newly measured performance of the final all-1,312-sample fit. Reference-equivalent
outputs and serialization checks do not establish accuracy on unseen tweets. The old
SVM benchmark test results do not apply to this replacement.

Rollback is explicit only: archived code and old model packages remain available.
Restore the previously reviewed adapter/config/processor policy together if rollback
is required; never silently load an old model on final-model failure.
