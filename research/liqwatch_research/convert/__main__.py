import argparse
import time

from . import convert


def main():
    p = argparse.ArgumentParser()
    p.add_argument('files', nargs='+')
    p.add_argument('--out', required=True)
    a = p.parse_args()
    t = time.time()
    written, stats, files = convert(a.files, a.out)
    print(f'rows written: {written}')
    print(f'files: {files} duplicates dropped: {stats.duplicates} torn lines: {stats.torn_lines} '
          f'order violations: {stats.order_violations} seconds: {time.time() - t:.1f}')


if __name__ == '__main__':
    main()
