"""Validated adapter for the private scikit-learn baseline artifacts."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import io
import json
import math
from pathlib import Path
from typing import Any


EXPECTED_CLASSES = {
    "relevance": frozenset({"disruption", "not_disruption"}),
    "severity": frozenset({"light", "moderate", "severe"}),
}


class ModelArtifactError(RuntimeError):
    """A model could not be loaded or returned an invalid prediction."""


@dataclass(frozen=True)
class ModelPrediction:
    label: str
    probabilities: dict[str, float]
    confidence: float
    model_version: str
    artifact_sha256: str


@dataclass(frozen=True)
class _LoadedModel:
    key: str
    version: str
    path: Path
    digest: str | None
    estimator: Any | None
    error_type: str | None


class ClassifierAdapter:
    """Load each model once and validate class and prediction contracts."""

    def __init__(self, relevance: _LoadedModel, severity: _LoadedModel) -> None:
        self._models = {"relevance": relevance, "severity": severity}

    @classmethod
    def load(cls, artifact_dir: Path | None = None) -> ClassifierAdapter:
        if artifact_dir is None:
            artifact_dir = (
                Path(__file__).resolve().parents[3]
                / "evaluation/private/Scrape_Manual/baseline_artifacts"
            )

        metrics_path = artifact_dir / "metrics.json"
        relevance_path = artifact_dir / "relevance_tfidf_logreg.joblib"
        severity_path = artifact_dir / "severity_tfidf_logreg.joblib"
        relevance_payload, relevance_digest = cls._read_artifact(relevance_path)
        severity_payload, severity_digest = cls._read_artifact(severity_path)
        try:
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            experiment = metrics["experiment"]
            recorded_sklearn_version = metrics["scikit_learn_version"]
            if not isinstance(experiment, str) or not experiment.strip():
                raise ValueError("invalid experiment version")
            if not isinstance(recorded_sklearn_version, str):
                raise ValueError("missing scikit-learn version")
        except Exception as exc:
            # Keep the adapter constructible so the claimed attempt can be
            # finished with a distinct model error instead of a decision.
            error_type = type(exc).__name__
            return cls(
                _LoadedModel(
                    "relevance", "unavailable", relevance_path,
                    relevance_digest, None, error_type,
                ),
                _LoadedModel(
                    "severity", "unavailable", severity_path,
                    severity_digest, None, error_type,
                ),
            )

        try:
            import sklearn
            import joblib

            if sklearn.__version__ != recorded_sklearn_version:
                raise RuntimeError("scikit-learn runtime version mismatch")
        except Exception as exc:
            error_type = type(exc).__name__
            return cls(
                _LoadedModel(
                    "relevance", f"{experiment}:relevance", relevance_path,
                    relevance_digest, None, error_type,
                ),
                _LoadedModel(
                    "severity", f"{experiment}:severity", severity_path,
                    severity_digest, None, error_type,
                ),
            )

        return cls(
            cls._load_one(
                "relevance",
                f"{experiment}:relevance",
                relevance_path,
                relevance_payload,
                relevance_digest,
            ),
            cls._load_one(
                "severity",
                f"{experiment}:severity",
                severity_path,
                severity_payload,
                severity_digest,
            ),
        )

    @staticmethod
    def _read_artifact(path: Path) -> tuple[bytes | None, str | None]:
        try:
            payload = path.read_bytes()
            return payload, hashlib.sha256(payload).hexdigest()
        except OSError:
            return None, None

    @staticmethod
    def _load_one(
        key: str,
        version: str,
        path: Path,
        payload: bytes | None,
        digest: str | None,
    ) -> _LoadedModel:
        try:
            if payload is None or digest is None:
                raise FileNotFoundError("model artifact is unavailable")
            import joblib

            estimator = joblib.load(io.BytesIO(payload))
            expected = EXPECTED_CLASSES[key]
            classes = getattr(estimator, "classes_", None)
            if classes is None or frozenset(str(value) for value in classes) != expected:
                raise ValueError("unexpected model classes")
            if not callable(getattr(estimator, "predict", None)):
                raise ValueError("model has no predict method")
            if not callable(getattr(estimator, "predict_proba", None)):
                raise ValueError("model has no predict_proba method")
            return _LoadedModel(key, version, path, digest, estimator, None)
        except Exception as exc:
            return _LoadedModel(key, version, path, digest, None, type(exc).__name__)

    @property
    def artifact_manifest(self) -> dict[str, dict[str, str | None]]:
        return {
            key: {
                "model_version": model.version,
                "artifact_sha256": model.digest,
                "status": "loaded" if model.estimator is not None else "unavailable",
                "error_type": model.error_type,
            }
            for key, model in self._models.items()
        }

    def predict_relevance(self, text: str) -> ModelPrediction:
        return self._predict("relevance", text)

    def predict_severity(self, text: str) -> ModelPrediction:
        return self._predict("severity", text)

    def _predict(self, key: str, text: str) -> ModelPrediction:
        model = self._models[key]
        if model.estimator is None or model.digest is None:
            raise ModelArtifactError(f"{key} model unavailable: {model.error_type or 'unknown'}")
        if not isinstance(text, str) or not text.strip():
            raise ModelArtifactError("model input is empty or invalid")

        try:
            classes = [str(value) for value in model.estimator.classes_]
            if frozenset(classes) != EXPECTED_CLASSES[key]:
                raise ValueError("unexpected model classes")
            predicted = model.estimator.predict([text])
            raw_probabilities = model.estimator.predict_proba([text])
            if len(predicted) != 1:
                raise ValueError("unexpected prediction count")
            matrix = (
                raw_probabilities.tolist()
                if hasattr(raw_probabilities, "tolist")
                else raw_probabilities
            )
            if len(matrix) != 1 or len(matrix[0]) != len(classes):
                raise ValueError("unexpected probability shape")

            scores = [float(value) for value in matrix[0]]
            if (
                any(not math.isfinite(value) or value < 0 or value > 1 for value in scores)
                or not math.isclose(sum(scores), 1.0, rel_tol=1e-5, abs_tol=1e-6)
            ):
                raise ValueError("invalid probability values")
            label = str(predicted[0])
            if label not in EXPECTED_CLASSES[key]:
                raise ValueError("unexpected predicted label")
            if label != classes[scores.index(max(scores))]:
                raise ValueError("predicted label disagrees with class scores")
            probabilities = dict(zip(classes, scores, strict=True))
            return ModelPrediction(
                label=label,
                probabilities=probabilities,
                confidence=probabilities[label],
                model_version=model.version,
                artifact_sha256=model.digest,
            )
        except Exception as exc:
            raise ModelArtifactError(f"{key} prediction invalid: {type(exc).__name__}") from exc
