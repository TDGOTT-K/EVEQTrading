from __future__ import annotations

import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .errors import EveSsoCallbackError


def wait_for_authorization_code(callback_url: str, expected_state: str, timeout_seconds: float) -> str:
    parsed = urlparse(callback_url)
    if parsed.scheme != "http":
        raise EveSsoCallbackError(f"Only http callback URLs are supported for local login: {callback_url}")
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 80
    path = parsed.path or "/"

    result: dict[str, object] = {}
    finished = threading.Event()

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            request_path = urlparse(self.path)
            if request_path.path != path:
                self.send_response(404)
                self.send_header("content-type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"Not found")
                return

            query = parse_qs(request_path.query)
            if query.get("state", [None])[0] != expected_state:
                self.send_response(400)
                self.send_header("content-type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"State mismatch")
                result["error"] = EveSsoCallbackError("EVE SSO callback state mismatch.")
                finished.set()
                return

            error_code = query.get("error", [None])[0]
            if error_code:
                description = query.get("error_description", [""])[0]
                self.send_response(400)
                self.send_header("content-type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(f"SSO error: {error_code}".encode("utf-8"))
                result["error"] = EveSsoCallbackError(
                    f"EVE SSO login failed: {error_code} {description}".strip()
                )
                finished.set()
                return

            code = query.get("code", [None])[0]
            if not code:
                self.send_response(400)
                self.send_header("content-type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"Missing authorization code")
                result["error"] = EveSsoCallbackError("EVE SSO callback is missing the authorization code.")
                finished.set()
                return

            self.send_response(200)
            self.send_header("content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                b"<!doctype html><html><body><h1>EVE SSO login complete</h1><p>You can close this window.</p></body></html>"
            )
            result["code"] = code
            finished.set()

        def log_message(self, format: str, *args: object) -> None:
            return

    try:
        server = ThreadingHTTPServer((host, port), CallbackHandler)
    except OSError as error:
        raise EveSsoCallbackError(f"Failed to listen for EVE SSO callback on {callback_url}: {error}") from error

    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + timeout_seconds
        while not finished.wait(timeout=0.2):
            if time.monotonic() >= deadline:
                raise EveSsoCallbackError(f"Timed out waiting for EVE SSO callback after {timeout_seconds}s.")
        if "error" in result:
            error = result["error"]
            if isinstance(error, Exception):
                raise error
            raise EveSsoCallbackError(str(error))
        code = result.get("code")
        if not isinstance(code, str):
            raise EveSsoCallbackError("EVE SSO callback listener stopped without receiving a code.")
        return code
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1.0)
