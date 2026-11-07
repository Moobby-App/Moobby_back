"""Verify cached artifacts, or print an audited manifest. Never downloads/writes."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from mooby.worker.bundle_manifest import build_manifest, bundle_files, verify_bundle

ROOT = Path(__file__).resolve().parents[1]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def audit_cache(path, lock):
    """Bind local HF snapshot layout to revision and content-addressed blobs.

    This checks local provenance, not a remote signature or a fresh Hub listing.
    """
    require(path.name == lock['revision'], 'snapshot revision differs')
    require(path.parent.name == 'snapshots', 'unexpected snapshot layout')
    repo = path.parent.parent
    require(repo.name == 'models--' + lock['model_id'].replace('/', '--'), 'model differs')
    files = bundle_files(path)
    require(len(files) == lock['file_count'], 'unexpected inventory')
    for f in files:
        require(f.is_symlink(), 'expected content-addressed HF cache symlink')
        target = f.resolve(strict=True)
        require(target.parent == repo / 'blobs', 'unexpected blob location')
        key = target.name
        require(len(key) in (40, 64), 'unknown blob hash format')
        digest = hashlib.sha1() if len(key) == 40 else hashlib.sha256()
        if len(key) == 40:
            digest.update(f'blob {f.stat().st_size}\0'.encode('ascii'))
        with f.open('rb') as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b''):
                digest.update(chunk)
        require(digest.hexdigest() == key, 'cache blob hash differs')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit-cache', action='store_true',
                        help='print reproducible manifest after checking cached blob provenance')
    args = parser.parse_args()
    from huggingface_hub import snapshot_download
    lock = json.loads((ROOT/'evaluation/embedding_model.lock.json').read_text())
    path = Path(snapshot_download(lock['model_id'], revision=lock['revision'], local_files_only=True))
    if args.audit_cache:
        audit_cache(path, lock)
        print(json.dumps(build_manifest(path, lock['model_id'], lock['revision']),
                         indent=2, ensure_ascii=False))
    else:
        manifest = json.loads((ROOT/'evaluation'/lock['manifest_file']).read_text())
        digest = verify_bundle(path, manifest, lock)
        print(json.dumps({'status': 'verified', 'file_count': len(manifest['files']),
                          'digest_method': lock['digest_method'], 'artifact_sha256': digest}))


if __name__ == '__main__':
    main()
