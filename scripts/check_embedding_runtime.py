"""Offline-only model smoke test. Does not read source data or connect to DB."""
from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from mooby.worker.embedding import embed_event, EmbeddingUnavailable


def main():
    try:
        result = embed_event('pemeriksaan model embedding lokal')
    except EmbeddingUnavailable as exc:
        print(json.dumps({'status': 'unavailable', 'reason': str(exc)}))
        return 1
    print(json.dumps({'status': 'available', 'dimension': len(result.values),
                      'input_version': result.input_version,
                      'model_version': result.model_version,
                      'model_digest': result.model_digest}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
