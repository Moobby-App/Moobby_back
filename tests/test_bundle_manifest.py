"""Deterministic temporary-file bundle tests; never touches the model cache."""
import json
from pathlib import Path
import tempfile
import unittest

from mooby.worker.bundle_manifest import (
    DIGEST_METHOD, BundleValidationError, build_manifest, manifest_digest, verify_bundle)


class BundleManifestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)/'bundle'
        self.root.mkdir()
        (self.root/'z').write_bytes(b'weights for testing only')
        (self.root/'.hidden').mkdir()
        (self.root/'.hidden'/'a').write_bytes(b'config')
        self.manifest = build_manifest(self.root, 'test/model', 'revision')
        self.lock = dict(model_id='test/model', revision='revision', file_count=2,
                         digest_method=DIGEST_METHOD,
                         artifact_sha256=manifest_digest(self.manifest))

    def verify(self):
        return verify_bundle(self.root, self.manifest, self.lock)

    def test_deterministic_across_creation_order_location_and_json_format(self):
        other = Path(self.temp.name)/'second'; other.mkdir()
        (other/'.hidden').mkdir()
        (other/'.hidden'/'a').write_bytes(b'config')
        (other/'z').write_bytes(b'weights for testing only')
        manifest = build_manifest(other, 'test/model', 'revision')
        self.assertEqual(self.manifest, manifest)
        reordered = {k: manifest[k] for k in reversed(list(manifest))}
        self.assertEqual(self.verify(), manifest_digest(json.loads(json.dumps(reordered, indent=4))))
        self.assertEqual([e['path'] for e in manifest['files']], ['.hidden/a', 'z'])

    def test_changed_file_fails(self):
        (self.root/'z').write_bytes(b'changed')
        with self.assertRaises(BundleValidationError): self.verify()

    def test_missing_file_fails(self):
        (self.root/'z').unlink()
        with self.assertRaises(BundleValidationError): self.verify()

    def test_added_file_even_under_cache_fails(self):
        (self.root/'.cache').mkdir()
        (self.root/'.cache'/'extra').write_bytes(b'extra')
        with self.assertRaises(BundleValidationError): self.verify()

    def test_renamed_file_fails(self):
        (self.root/'z').rename(self.root/'renamed')
        with self.assertRaises(BundleValidationError): self.verify()

    def test_file_symlink_hashes_target_content_without_target_path(self):
        target = Path(self.temp.name)/'external-blob'
        target.write_bytes((self.root/'z').read_bytes())
        (self.root/'z').unlink()
        (self.root/'z').symlink_to(target)
        self.assertEqual(self.verify(), self.lock['artifact_sha256'])
        self.assertNotIn(str(target), json.dumps(self.manifest))
        target.write_bytes(b'modified target')
        with self.assertRaises(BundleValidationError): self.verify()

    def test_broken_or_directory_symlinks_fail(self):
        extra = self.root/'extra'
        extra.symlink_to(self.root/'missing')
        with self.assertRaises(BundleValidationError): self.verify()
        extra.unlink()
        extra.symlink_to(self.root/'.hidden', target_is_directory=True)
        with self.assertRaises(BundleValidationError): self.verify()

    def test_manifest_tampering_or_lock_mismatch_fails(self):
        for field, value in [('revision', 'other'), ('model_id', 'other/model'),
                             ('file_count', 3), ('digest_method', 'old'),
                             ('artifact_sha256', '0'*64)]:
            bad = {**self.lock, field: value}
            with self.subTest(field=field), self.assertRaises(BundleValidationError):
                verify_bundle(self.root, self.manifest, bad)
        altered = json.loads(json.dumps(self.manifest))
        altered['files'][0]['sha256'] = '0'*64
        with self.assertRaises(BundleValidationError):
            verify_bundle(self.root, altered, self.lock)

    def test_manifest_paths_must_be_relative_unique_canonical_and_sorted(self):
        for bad_name in ['/absolute', '../outside', 'a//b', './a', 'a\\b']:
            altered = json.loads(json.dumps(self.manifest))
            altered['files'][0]['path'] = bad_name
            with self.subTest(path=bad_name), self.assertRaises(BundleValidationError):
                manifest_digest(altered)
        for entries in [self.manifest['files'][::-1], self.manifest['files']*2]:
            with self.assertRaises(BundleValidationError):
                manifest_digest({**self.manifest, 'files': entries})

    def test_serialization_has_versioned_domain_canonical_json_and_lf(self):
        import hashlib
        payload = json.dumps(self.manifest, sort_keys=True, ensure_ascii=False,
                             separators=(',', ':'), allow_nan=False).encode('utf-8')
        expected = hashlib.sha256(DIGEST_METHOD.encode('ascii')+b'\n'+payload+b'\n').hexdigest()
        self.assertEqual(self.verify(), expected)
