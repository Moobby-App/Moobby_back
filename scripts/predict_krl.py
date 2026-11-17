"""Offline final classifier: read one JSON string or ordered string array on stdin."""
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from mooby.worker.classifier import ClassifierAdapter, ModelArtifactError


def main():
    try:
        value = json.load(sys.stdin)
        classifier = ClassifierAdapter.load()
        if any(item['status'] != 'loaded' for item in classifier.artifact_manifest.values()):
            raise ModelArtifactError('model unavailable')
        result = classifier.predict_many(value) if isinstance(value, list) else classifier.predict(value)
    except ModelArtifactError:
        print(json.dumps({'error': 'model_unavailable'}))
        return 1
    except (TypeError, ValueError):
        print(json.dumps({'error': 'invalid_input'}))
        return 2
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
