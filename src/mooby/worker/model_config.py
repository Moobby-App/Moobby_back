"""Final Hybrid TF-IDF v1.0. Restart API/mock and worker to reload artifacts."""

from pathlib import Path


ACTIVE_CLASSIFIER_ARTIFACT_DIR = (
    Path(__file__).resolve().parent / "artifacts/krl_hybrid_final_v1"
)
USES_PROBABILITY_THRESHOLDS = False

# Archive only; the active adapter never falls back to this directory.
LEGACY_CLASSIFIER_ARTIFACT_DIR = (
    Path(__file__).resolve().parents[3]
    / "evaluation/private/model_artifacts/relevance_svm_sigmoid_v1"
)
