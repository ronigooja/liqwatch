import hashlib
import os
import re
import time
import warnings
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import requests

DEFAULT_BASE = "https://icandrinkmore.beer/dav/ronigooja_onedrive/wicker/"


@dataclass(frozen=True)
class RemoteFile:
    date: str
    name: str
    meta: dict
    chunks: tuple


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


class Client:
    def __init__(self, base=None, user=None, password=None, retries=4, backoff=0.2):
        self.base = (base or os.environ.get("WICKER_WEBDAV_BASE", DEFAULT_BASE)).rstrip("/") + "/"
        self.session = requests.Session()
        user = user if user is not None else os.environ.get("WICKER_WEBDAV_USER")
        password = password if password is not None else os.environ.get("WICKER_WEBDAV_PASS")
        if user is not None and password is not None:
            self.session.auth = (user, password)
        self.retries, self.backoff = retries, backoff

    def request(self, method, path, **kwargs):
        url = self.base + "/".join(quote(p, safe="") for p in path.split("/"))
        for attempt in range(self.retries):
            try:
                response = self.session.request(method, url, timeout=(10, 60), **kwargs)
                if response.status_code < 500 or attempt == self.retries - 1:
                    response.raise_for_status()
                    return response
                response.close()
            except (requests.Timeout, requests.ConnectionError):
                if attempt == self.retries - 1:
                    raise
            time.sleep(self.backoff * 2 ** attempt)

    def list_remote(self, date):
        if not re.fullmatch(r"\d{8}", date):
            raise ValueError("date must be YYYYMMDD")
        directory = f"raw/{date}"
        response = self.request("PROPFIND", directory + "/", headers={"Depth": "1"})
        sizes = {}
        root = ET.fromstring(response.content)
        for item in root.findall("{DAV:}response"):
            href = item.findtext("{DAV:}href", "")
            name = unquote(urlsplit(href).path.rstrip("/").split("/")[-1])
            for propstat in item.findall("{DAV:}propstat"):
                if " 200 " not in propstat.findtext("{DAV:}status", ""):
                    continue
                size = propstat.findtext("{DAV:}prop/{DAV:}getcontentlength")
                if size is not None:
                    sizes[name] = int(size)
        files = []
        for meta_name in sorted(n for n in sizes if n.endswith(".meta.json")):
            name = meta_name[:-len(".meta.json")]
            try:
                meta = self.request("GET", f"{directory}/{meta_name}").json()
                chunks = meta["chunks"]
                if (not isinstance(chunks, list) or
                    any(chunk != f"{name}.part{i:04d}" for i, chunk in enumerate(chunks)) or
                    not re.fullmatch(r"[0-9a-f]{64}", meta["sha256"]) or
                    not isinstance(meta["size"], int) or meta["size"] < 0):
                    raise ValueError("invalid metadata")
                parts = tuple((chunk, sizes[chunk]) for chunk in chunks)
                if sum(size for _, size in parts) != meta["size"]:
                    raise ValueError("chunk size mismatch")
            except (ValueError, TypeError, KeyError):
                warnings.warn(f"skipping incomplete or invalid metadata: {name}")
                continue
            files.append(RemoteFile(date, name, meta, parts))
        return files

    def fetch(self, file, dest):
        dest = Path(dest)
        if dest.is_file() and dest.stat().st_size == file.meta["size"] and digest(dest) == file.meta["sha256"]:
            return False
        dest.parent.mkdir(parents=True, exist_ok=True)
        partial = Path(str(dest) + ".partial")
        with open(partial, "a+b") as out:
            out.seek(0, os.SEEK_END)
            existing = out.tell()
            offset = 0
            for name, size in file.chunks:
                if existing >= offset + size:
                    offset += size
                    continue
                out.truncate(offset)
                out.seek(offset)
                for attempt in range(self.retries):
                    try:
                        with self.request("GET", f"raw/{file.date}/{name}", stream=True) as response:
                            for block in response.iter_content(1024 * 1024):
                                out.write(block)
                        if out.tell() - offset != size:
                            raise requests.ConnectionError("incomplete chunk")
                        break
                    except (requests.Timeout, requests.ConnectionError, requests.ChunkedEncodingError):
                        out.seek(offset)
                        out.truncate()
                        if attempt == self.retries - 1:
                            raise
                        time.sleep(self.backoff * 2 ** attempt)
                offset += size
            out.truncate(offset)
        if partial.stat().st_size != file.meta["size"] or digest(partial) != file.meta["sha256"]:
            partial.unlink()
            raise ValueError(f"sha256 mismatch for {file.name}")
        os.replace(partial, dest)
        return True


def list_remote(date):
    return Client().list_remote(date)


def fetch(file, dest):
    return Client().fetch(file, dest)
