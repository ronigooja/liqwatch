"""Recover compact raw-record identity; legacy envelopes remain self-describing."""
import re
from pathlib import Path


def with_identity(record, path):
    if all(key in record for key in ('venue', 'ch', 'sym')):
        return record
    # Legacy research files did not always carry venue; preserve their identity.
    if record.get('v', 1) == 1 and 'ch' in record and 'sym' in record:
        return record
    parts = Path(path).parts
    name = parts[-1].removesuffix('.zst').removesuffix('.jsonl')
    if len(parts) < 4 or not re.fullmatch(r'\d{8}', parts[-2]):
        raise ValueError(f'raw record needs venue/symbol/date/channel path: {path}')
    if not str(path).removesuffix('.zst').endswith('.jsonl'):
        raise ValueError(f'not a raw JSONL path: {path}')
    channel = re.sub(r'\.[a-z]{1,8}$', '', name)
    symbol = parts[-3]
    return dict(record, venue=parts[-4], ch=channel,
                sym='' if symbol in ('_all', 'gap') else symbol)
