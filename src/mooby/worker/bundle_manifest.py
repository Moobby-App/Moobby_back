"""Versioned, deterministic model-bundle manifests; standard library only."""
import hashlib
import json
from pathlib import Path, PurePosixPath
import stat

DIGEST_METHOD = 'sha256-model-bundle-manifest-json-v1'
MANIFEST_FORMAT = 'model-bundle-manifest-v1'


class BundleValidationError(ValueError):
    pass


def file_sha256(path):
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def bundle_files(root):
    """All regular files, including hidden files; dereference file symlinks only.

    Empty directories are not represented. Directory/broken symlinks and special
    files fail closed. Symlink target paths never enter the manifest.
    """
    root = Path(root)
    if not root.is_dir():
        raise BundleValidationError('bundle directory unavailable')
    found = []

    def visit(directory):
        for path in directory.iterdir():
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                if not path.is_file():
                    raise BundleValidationError('non-file or broken bundle symlink')
                found.append(path)
            elif stat.S_ISDIR(mode):
                visit(path)
            elif stat.S_ISREG(mode):
                found.append(path)
            else:
                raise BundleValidationError('special bundle file')
    visit(root)
    return sorted(found, key=lambda path: path.relative_to(root).as_posix())


def build_manifest(root, model_id, revision):
    root = Path(root)
    return {
        'format': MANIFEST_FORMAT,
        'model_id': model_id,
        'revision': revision,
        'files': [{'path': f.relative_to(root).as_posix(), 'sha256': file_sha256(f)}
                  for f in bundle_files(root)],
    }


def validate_manifest(manifest):
    if (set(manifest) != {'format', 'model_id', 'revision', 'files'}
        or manifest['format'] != MANIFEST_FORMAT
        or not isinstance(manifest['model_id'], str) or not manifest['model_id']
        or not isinstance(manifest['revision'], str) or not manifest['revision']
        or not isinstance(manifest['files'], list) or not manifest['files']):
        raise BundleValidationError('invalid manifest structure')
    names = []
    for entry in manifest['files']:
        if not isinstance(entry, dict) or set(entry) != {'path', 'sha256'}:
            raise BundleValidationError('invalid manifest entry')
        name, digest = entry['path'], entry['sha256']
        if (not isinstance(name, str) or not name or '\\' in name or '\0' in name
            or PurePosixPath(name).is_absolute() or '..' in PurePosixPath(name).parts
            or PurePosixPath(name).as_posix() != name or name == '.'):
            raise BundleValidationError('invalid manifest path')
        if (not isinstance(digest, str) or len(digest) != 64
            or any(c not in '0123456789abcdef' for c in digest)):
            raise BundleValidationError('invalid manifest file hash')
        names.append(name)
    if names != sorted(set(names)):
        raise BundleValidationError('manifest entries must be unique and sorted')


def manifest_digest(manifest):
    """Hash domain prefix + canonical JSON + LF. See embedding_operations.md."""
    validate_manifest(manifest)
    payload = json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                         separators=(',', ':'), allow_nan=False).encode('utf-8')
    return hashlib.sha256(DIGEST_METHOD.encode('ascii') + b'\n' + payload + b'\n').hexdigest()


def verify_bundle(root, manifest, lock):
    digest = manifest_digest(manifest)
    if (lock['digest_method'] != DIGEST_METHOD
        or manifest['model_id'] != lock['model_id']
        or manifest['revision'] != lock['revision']
        or len(manifest['files']) != lock['file_count']
        or digest != lock['artifact_sha256']):
        raise BundleValidationError('manifest does not match model lock')
    actual = build_manifest(root, lock['model_id'], lock['revision'])
    if actual != manifest:
        raise BundleValidationError('bundle file set or content differs from manifest')
    return digest
