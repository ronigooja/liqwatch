import argparse
import json

import zstandard

from . import Validator


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--validate', nargs='+', required=True)
    parser.add_argument('--output', default='quality.parquet')
    args = parser.parse_args()
    validator = Validator()
    for path in args.validate:
        with zstandard.open(path, 'rt', encoding='utf-8') as source:
            for line in source:
                validator.feed(json.loads(line))
    summary = validator.finish()
    validator.write_parquet(args.output)
    for sym, s in sorted(summary.items()):
        rate = f"{s['passed'] / s['verifies']:.1%}" if s['verifies'] else 'n/a'
        print(f"{sym}: diffs={s['diffs']} seq_breaks={s['breaks']} verifies={s['verifies']} "
              f"pass_rate={rate} max_mismatch={s['max_mismatch']} "
              f"unavailable_seconds={s['unavailable_ns'] / 1e9:.3f} unverifiable_snapshots={s['unverifiable']} "
              f"inside_diff_checks={s['inside']} inside_diff_passed={s['inside_passed']}")


if __name__ == '__main__':
    main()
