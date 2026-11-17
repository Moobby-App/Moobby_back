"""Final Hybrid TF-IDF v1.0; no probability conversion or old-model fallback."""
from __future__ import annotations
from dataclasses import dataclass, field
import hashlib
import importlib.metadata
import io
import json
from pathlib import Path
import re
from typing import Any
from .model_config import ACTIVE_CLASSIFIER_ARTIFACT_DIR

FINAL_LABELS = frozenset({'not_disruption', 'light', 'moderate', 'heavy'})
STORAGE_SEVERITY = {'light': 'light', 'moderate': 'moderate', 'heavy': 'severe'}


class ModelArtifactError(RuntimeError):
    pass


def normalize_model_text(text: str) -> str:
    if not isinstance(text, str):
        raise TypeError('Input must be a string.')
    return re.sub(r'\s+', ' ', text).strip()


def _input(text: str) -> str:
    normalized = normalize_model_text(text)
    if not normalized:
        raise ValueError('Tweet cannot be empty.')
    return normalized


@dataclass(frozen=True)
class ModelPrediction:
    label: str
    probabilities: dict[str, float] | None
    confidence: float | None
    model_version: str
    artifact_sha256: str
    model_label: str | None = None


@dataclass(frozen=True)
class _LoadedStage:
    estimator: Any = field(repr=False)
    digest: str | None
    error_type: str | None = None


class ClassifierAdapter:
    uses_probability_thresholds = False

    def __init__(self, stage1: _LoadedStage, stage2: _LoadedStage, version: str):
        self.stage1, self.stage2, self.version = stage1, stage2, version

    @classmethod
    def load(cls, artifact_dir: Path | None = None) -> ClassifierAdapter:
        directory = Path(artifact_dir) if artifact_dir is not None else ACTIVE_CLASSIFIER_ARTIFACT_DIR
        digests = {}
        version = 'krl_hybrid_final_v1:unavailable'
        try:
            manifest = json.loads((directory / 'artifact_manifest.json').read_text())
            metadata = json.loads((directory / 'metadata.json').read_text())
            if metadata['model_version'] != '1.0' or set(metadata['labels']) != FINAL_LABELS:
                raise ValueError('unsupported model metadata')
            version = 'krl_hybrid_final_v1.0'
            for package, expected in manifest['runtime_requirements'].items():
                if importlib.metadata.version(package) != expected:
                    raise RuntimeError('classifier dependency mismatch')
            payloads = {}
            for name in ('metadata.json', 'requirements.txt', 'stage1.joblib', 'stage2.joblib'):
                payload = (directory / name).read_bytes()
                digest = hashlib.sha256(payload).hexdigest()
                if manifest['sha256'][name] != digest:
                    raise ValueError('classifier artifact checksum mismatch')
                payloads[name], digests[name] = payload, digest
            import joblib
            import numpy as np
            stages = [joblib.load(io.BytesIO(payloads[name])) for name in ('stage1.joblib', 'stage2.joblib')]
            if (set(stages[0].classes_) != {0, 1}
                    or any(not isinstance(v, (int, np.integer)) for v in stages[0].classes_)
                    or set(stages[1].classes_) != {'light', 'moderate', 'heavy'}):
                raise ValueError('unexpected classifier classes')
            if any(not callable(getattr(stage, 'predict', None)) for stage in stages):
                raise ValueError('missing prediction method')
            return cls(_LoadedStage(stages[0], digests['stage1.joblib']),
                       _LoadedStage(stages[1], digests['stage2.joblib']), version)
        except Exception as exc:
            return cls(_LoadedStage(None, digests.get('stage1.joblib'), type(exc).__name__),
                       _LoadedStage(None, digests.get('stage2.joblib'), type(exc).__name__), version)

    @property
    def artifact_manifest(self) -> dict[str, dict]:
        return {key: {
            'model_version': f'{self.version}:{key}', 'artifact_sha256': stage.digest,
            'status': 'loaded' if stage.estimator is not None else 'unavailable',
            'error_type': stage.error_type, 'probabilities_available': False,
        } for key, stage in (('relevance', self.stage1), ('severity', self.stage2))}

    def _predict_stage(self, stage: _LoadedStage, text: str, allowed):
        if stage.estimator is None or stage.digest is None:
            raise ModelArtifactError('final hybrid classifier unavailable')
        try:
            values = stage.estimator.predict([text])
            if len(values) != 1 or values[0] not in allowed:
                raise ValueError('invalid classifier output')
            return values[0]
        except Exception as exc:
            raise ModelArtifactError(f'final hybrid prediction invalid: {type(exc).__name__}') from exc

    def predict_relevance(self, raw_text: str) -> ModelPrediction:
        value = self._predict_stage(self.stage1, _input(raw_text), {0, 1})
        label = 'disruption' if value == 1 else 'not_disruption'
        return ModelPrediction(label, None, None, f'{self.version}:relevance', self.stage1.digest,
                               model_label=label)

    def predict_severity(self, raw_text: str) -> ModelPrediction:
        label = str(self._predict_stage(self.stage2, _input(raw_text), {'light', 'moderate', 'heavy'}))
        # Existing DB enum and public API use severe. Preserve the original label.
        return ModelPrediction(STORAGE_SEVERITY[label], None, None,
                               f'{self.version}:severity', self.stage2.digest, model_label=label)

    def predict(self, raw_text: str) -> dict[str, Any]:
        text = _input(raw_text)
        if self.predict_relevance(text).label == 'not_disruption':
            return {'prediction': 'not_disruption', 'is_disruption': False}
        return {'prediction': self.predict_severity(text).model_label, 'is_disruption': True}

    def predict_many(self, raw_texts: list[str]) -> list[dict[str, Any]]:
        if not isinstance(raw_texts, (list, tuple)):
            raise TypeError('Batch must be a list or tuple of strings.')
        texts = [_input(text) for text in raw_texts]
        return [self.predict(text) for text in texts]
