import argparse
import json
from . import Replay


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stats', action='store_true', required=True)
    parser.add_argument('files', nargs='+')
    args = parser.parse_args()
    replay = Replay(args.files)
    for _ in replay:
        pass
    s = replay.stats
    print(json.dumps(dict(channels=dict(s.channels), duplicates_dropped=s.duplicates, torn_lines=s.torn_lines, rw_range=[s.min_rw, s.max_rw], monotonic_order_violations=s.order_violations), sort_keys=True))


if __name__ == '__main__':
    main()
