import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from liqwatch_research.fetch import Client


@pytest.fixture
def server():
    name = "trade-2026110617.b.jsonl.zst"
    parts = [b"first", b"second", b"third"]
    data = b"".join(parts)
    objects = {f"{name}.part{i:04d}": part for i, part in enumerate(parts)}
    objects[name + ".meta.json"] = json.dumps({"size": len(data), "sha256": hashlib.sha256(data).hexdigest(), "chunks": list(objects)}).encode()
    hits = []
    failures = {}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_PROPFIND(self):
            assert self.headers["Depth"] == "1"
            entries = "".join(f"<d:response><d:href>/raw/20261106/{n}</d:href><d:propstat><d:prop><d:getcontentlength>{len(b)}</d:getcontentlength></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>" for n, b in objects.items())
            body = f'<d:multistatus xmlns:d="DAV:">{entries}</d:multistatus>'.encode()
            self.send_response(207)
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            key = self.path.rsplit("/", 1)[-1]
            hits.append(key)
            if failures.get(key, 0):
                failures[key] -= 1
                self.send_response(503)
                self.end_headers()
                return
            body = objects[key]
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield Client(f"http://127.0.0.1:{httpd.server_port}/", backoff=0.001), name, data, objects, hits, failures
    httpd.shutdown()
    thread.join()
    httpd.server_close()


def test_reassembly_and_skip(server, tmp_path):
    client, name, data, objects, hits, failures = server
    file, = client.list_remote("20261106")
    dest = tmp_path / name
    assert client.fetch(file, dest)
    assert dest.read_bytes() == data
    hits.clear()
    assert not client.fetch(file, dest)
    assert hits == []


def test_bad_hash_removes_partial(server, tmp_path):
    client, name, data, objects, hits, failures = server
    meta = json.loads(objects[name + ".meta.json"])
    meta["sha256"] = "0" * 64
    objects[name + ".meta.json"] = json.dumps(meta).encode()
    file, = client.list_remote("20261106")
    with pytest.raises(ValueError, match="sha256 mismatch"):
        client.fetch(file, tmp_path / name)
    assert not (tmp_path / (name + ".partial")).exists()
    assert not (tmp_path / name).exists()


def test_resume_at_complete_chunk_boundary(server, tmp_path):
    client, name, data, objects, hits, failures = server
    file, = client.list_remote("20261106")
    (tmp_path / (name + ".partial")).write_bytes(b"firstsec")
    hits.clear()
    client.fetch(file, tmp_path / name)
    assert (tmp_path / name).read_bytes() == data
    assert name + ".part0000" not in hits
    assert name + ".part0001" in hits


def test_retry_503(server, tmp_path):
    client, name, data, objects, hits, failures = server
    file, = client.list_remote("20261106")
    failures[name + ".part0000"] = 1
    client.fetch(file, tmp_path / name)
    assert hits.count(name + ".part0000") == 2
    assert (tmp_path / name).read_bytes() == data
