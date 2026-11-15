# Frozen-v2 TF-IDF + Linear SVM readiness

`benchmark_tfidf_linear_svm.py` is independent of `retrain.ipynb`.
Running it only validates inputs, constructs unfitted pipelines, and saves readiness
JSON and three figures in a fresh `evaluation/private/benchmark_readiness/` directory.
No `.fit()`, `.fit_transform()`, predictions, metrics, resampling, or splitting runs.

Input: `evaluation/krl_tweets_clean.csv`. Historical normalization is NFKC,
casefold, whitespace collapse. Keep-first normalized-text selection reproduces the
historical 1,311-row population from 1,618 raw records; the excluded 307 are explicitly
reported. Membership, original text, model text and labels must exactly match the
hash-verified historical snapshot. No new deduplication policy is introduced.

Final assignments come exclusively from
`evaluation/private/leakage_audit/final_split_v2_20261115T143218620874Z/`.
Join is one-to-one on string `tweet_id`, independent of row order. Frozen checksums,
ID sets, class counts, event separation and similarity groups are checked before output.

Future experiments: word TF-IDF with unigrams or unigrams+bigrams, sublinear TF,
L2 normalization, no stopword removal or stemming, `lowercase=False` because input is
already normalized; `LinearSVC` C in 0.1/1/10 and class weights None/balanced.
This gives 12 configurations per task, not a four-class classifier.
Relevance uses all training rows; severity uses only true-disruption training rows.
Each configuration/task needs a fresh pipeline fitted solely on training texts.
Audit vectorizers/matrices must never be reused as predictive features.

Select each task by validation macro-F1. Predetermined ties prefer lower C,
then unigrams, then no class weight. Later report per-class precision/recall/F1,
support and confusion matrices, especially severe recall. Severity evaluated on
true disruption is conditional performance, not end-to-end cascade performance.
Freeze the selected train-fit pipeline before one final test evaluation. No automatic
train+validation refit. Test labels/counts inspected for integrity do not select models.
No training runner is enabled in this preparation phase.

Event independence remains incomplete: frozen provenance retains 547 unresolved
cross-partition relationships involving 201 posts. Only 39 severe training examples
remain; validation/test severe supports are 13/14. Interpret later scores accordingly.
Readiness JSON records actual dependency versions and input hashes; no dependencies
are installed or project experiment settings changed by this script.

Run preparation only:
```sh
MPLCONFIGDIR=/tmp/moobby-mpl PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B evaluation/benchmark_tfidf_linear_svm.py
```
