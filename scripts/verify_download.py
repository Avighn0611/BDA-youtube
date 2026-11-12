"""Verify an Apache SHA-512 checksum before extracting a runtime archive."""
import hashlib
from pathlib import Path
import re
import sys

archive, checksum = map(Path, sys.argv[1:])
expected = re.search(r'\b[0-9a-fA-F]{128}\b', checksum.read_text())
if not expected:
    raise SystemExit('Could not parse the official SHA-512 checksum')
with archive.open('rb') as source:
    actual = hashlib.file_digest(source, 'sha512').hexdigest()
if actual.lower() != expected.group().lower():
    raise SystemExit('Checksum mismatch: remove the incomplete archive and retry')
print('Apache SHA-512 checksum verified')
