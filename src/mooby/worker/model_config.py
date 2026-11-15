"""Active classifier package. Changing this path requires restarting workers.

Relevance: TF-IDF + LinearSVC, train-only grouped sigmoid calibration.
Severity: original LogisticRegression artifact, unchanged.
"""

from pathlib import Path


ACTIVE_CLASSIFIER_ARTIFACT_DIR = (
    Path(__file__).resolve().parents[3]
    / "evaluation/private/model_artifacts/relevance_svm_sigmoid_v1"
)
