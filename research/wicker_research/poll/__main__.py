import argparse
import logging
from pathlib import Path

import requests

from . import BracketsStructureError, Config, Poller, RawWriter


def main():
    parser = argparse.ArgumentParser(prog='wickerpoll')
    parser.add_argument('--raw', required=True)
    parser.add_argument('--state', help='default: <raw>/poll-state.json')
    for name, field in Config.__dataclass_fields__.items():
        parser.add_argument('--' + name.replace('_', '-'), type=type(field.default), default=field.default)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    config = Config(**{name: getattr(args, name) for name in Config.__dataclass_fields__})
    writer = RawWriter(args.raw)
    try:
        with requests.Session() as session:
            Poller(session, writer, args.state or Path(args.raw) / 'poll-state.json', config).run()
    except KeyboardInterrupt:
        return 0
    except BracketsStructureError:
        return 1
    finally:
        writer.close()


if __name__ == '__main__':
    raise SystemExit(main())
