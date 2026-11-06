import argparse
import re
from pathlib import Path

from . import Client


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hours", nargs="+", default=None)
    parser.add_argument("--groups", nargs="+", default=None)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    if not args.out and not (args.list or args.dry_run):
        parser.error("--out is required unless using --list or --dry-run")
    if args.hours and any(not re.fullmatch(r"(?:[01]\d|2[0-3])", h) for h in args.hours):
        parser.error("hours must be two digits from 00 to 23")
    client = Client()
    for file in client.list_remote(args.date):
        match = re.match(r"^(.+?)-(\d{8})(\d{2})(?:\.|-)", file.name)
        if not match or match[2] != args.date:
            continue
        if args.groups and match[1] not in args.groups or args.hours and match[3] not in args.hours:
            continue
        if args.list or args.dry_run:
            print(f"{file.name} {file.meta['size']}")
        else:
            changed = client.fetch(file, args.out / file.name)
            print(f"{'fetched' if changed else 'verified'} {file.name}")


if __name__ == "__main__":
    main()
