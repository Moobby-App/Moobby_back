"""Local-only, pinned multilingual E5 provider. Never substitutes vectors."""
from dataclasses import dataclass, field
from functools import lru_cache
import json
import math
from pathlib import Path

from mooby.worker.bundle_manifest import verify_bundle

LOCK_PATH = Path(__file__).resolve().parents[3] / 'evaluation/embedding_model.lock.json'
INPUT_VERSION = 'nfkc-casefold-whitespace-v1:e5-passage-v1:l2-v1'
DIMENSION = 384


class EmbeddingUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class EventEmbedding:
    values: tuple[float, ...] = field(repr=False)
    model_version: str
    model_digest: str
    input_version: str = INPUT_VERSION

    def validate(self):
        if (len(self.values) != DIMENSION
            or any(not math.isfinite(v) for v in self.values)
            or not math.isclose(sum(v*v for v in self.values), 1, abs_tol=1e-4)
            or self.input_version != INPUT_VERSION
            or not self.model_version.strip()
            or len(self.model_digest) != 64
            or any(c not in '0123456789abcdef' for c in self.model_digest)):
            raise EmbeddingUnavailable('invalid embedding contract')

    def pgvector(self):
        self.validate()
        return '[' + ','.join(str(float(v)) for v in self.values) + ']'


@lru_cache(maxsize=1)
def _runtime():
    # Successful loads are cached; failures can recover on the next queue retry.
    import os
    from sentence_transformers import SentenceTransformer
    from huggingface_hub import snapshot_download
    lock = json.loads(LOCK_PATH.read_text())
    if lock['model_id'] != 'intfloat/multilingual-e5-small' or lock['embedding_dimension'] != DIMENSION:
        raise EmbeddingUnavailable('incompatible embedding lock')
    path = Path(os.environ['MOOBY_EMBEDDING_MODEL_DIR']) if os.environ.get('MOOBY_EMBEDDING_MODEL_DIR') else Path(snapshot_download(
        lock['model_id'], revision=lock['revision'], local_files_only=True))
    manifest_name = lock['manifest_file']
    if Path(manifest_name).name != manifest_name:
        raise EmbeddingUnavailable('invalid manifest filename')
    manifest = json.loads((LOCK_PATH.parent / manifest_name).read_text())
    digest = verify_bundle(path, manifest, lock)
    model = SentenceTransformer(str(path), device='cpu', local_files_only=True,
                                trust_remote_code=False)
    if model.get_sentence_embedding_dimension() != DIMENSION:
        raise EmbeddingUnavailable('incompatible model dimension')
    return model, lock['model_id'] + '@' + lock['revision'], digest


def embed_event(derived_text):
    try:
        if not derived_text.strip():
            raise EmbeddingUnavailable('empty embedding input')
        model, version, digest = _runtime()
        matrix = model.encode(['passage: ' + derived_text], normalize_embeddings=True,
                              convert_to_numpy=True, show_progress_bar=False)
        if len(matrix) != 1:
            raise EmbeddingUnavailable('invalid embedding shape')
        result = EventEmbedding(tuple(float(v) for v in matrix[0]), version, digest)
        result.validate()
        return result
    except EmbeddingUnavailable:
        raise
    except Exception as exc:
        # Do not log model exception messages, input text, or paths from secrets.
        raise EmbeddingUnavailable(type(exc).__name__) from None
