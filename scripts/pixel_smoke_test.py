#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def make_token(secret: str, tg_message_id: int) -> str:
    payload = b64url_encode(
        json.dumps({"tg": tg_message_id, "nonce": uuid4().hex}, separators=(",", ":")).encode()
    )
    signature = b64url_encode(hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest())
    return f"{payload}.{signature}"


def build_bundle(base_url: str, secret: str, tg_message_id: int) -> tuple[dict[str, str], str]:
    token = make_token(secret, tg_message_id)
    nonce = uuid4().hex[:10]
    base_url = base_url.rstrip("/")
    urls = {
        "img": f"{base_url}/track/img/2x1/{token}.png",
        "bg": f"{base_url}/track/bg/2x1/{token}.png",
        "dark": f"{base_url}/track/dark/2x1/{token}.png",
        "font": f"{base_url}/track/font/{token}.woff2",
    }
    html = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>GlassyReply Pixel Smoke</title>
    <style>
      @media (prefers-color-scheme: dark) {{
        .gr-dark-{nonce} {{
          background-image: url("{urls["dark"]}") !important;
        }}
      }}
      @font-face {{
        font-family: "grtrack-{nonce}";
        src: url("{urls["font"]}") format("woff2");
      }}
      body {{
        font-family: sans-serif;
      }}
      .gr-font-{nonce} {{
        font-family: "grtrack-{nonce}", sans-serif;
        font-size: 1px;
        line-height: 1px;
        color: transparent;
      }}
    </style>
  </head>
  <body>
    <p>GlassyReply pixel smoke fixture.</p>
    <img src="{urls["img"]}" width="2" height="1" alt="" style="display:block;opacity:0.01">
    <div style="background-image:url('{urls["bg"]}');background-repeat:no-repeat;background-size:2px 1px;width:2px;height:1px;opacity:0.01;">&nbsp;</div>
    <div class="gr-dark-{nonce}" style="background-image:url('{urls["bg"]}');background-repeat:no-repeat;background-size:2px 1px;width:2px;height:1px;opacity:0.01;">&nbsp;</div>
    <span class="gr-font-{nonce}">.</span>
  </body>
</html>
"""
    return urls, html


def cmd_bundle(args: argparse.Namespace) -> int:
    urls, html = build_bundle(args.base_url, args.secret, args.tg_id)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html)

    print(f"HTML fixture: {out_path}")
    for name, url in urls.items():
        print(f"{name}: {url}")
    print()
    print("Browser probe:")
    print(f"  open {out_path}")
    print()
    print("Direct curl probes:")
    print(
        "  curl -I -A 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        f"(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36' '{urls['img']}'"
    )
    print(
        "  curl -I -A 'GoogleImageProxy' "
        "-H 'X-Gmail-Fetch-Info: msgid=probe' "
        f"'{urls['img']}'"
    )
    return 0


def cmd_capture(args: argparse.Namespace) -> int:
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            if self.path != "/pixel_status":
                self.send_response(404)
                self.end_headers()
                return
            if args.secret and self.headers.get("X-Pixel-Secret") != args.secret:
                self.send_response(401)
                self.end_headers()
                return
            length = int(self.headers.get("Content-Length", "0"))
            payload = self.rfile.read(length).decode()
            with write_lock, out_path.open("a", encoding="utf-8") as handle:
                handle.write(payload + "\n")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Capture server listening on http://{args.host}:{args.port}/pixel_status")
    print(f"Writing events to {out_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    bundle = subparsers.add_parser("bundle")
    bundle.add_argument("--base-url", required=True)
    bundle.add_argument("--secret", required=True)
    bundle.add_argument("--tg-id", type=int, default=424242)
    bundle.add_argument(
        "--out",
        default="output/pixel-smoke/pixel-fixture.html",
    )
    bundle.set_defaults(func=cmd_bundle)

    capture = subparsers.add_parser("capture")
    capture.add_argument("--host", default="127.0.0.1")
    capture.add_argument("--port", type=int, default=8788)
    capture.add_argument("--secret", default="")
    capture.add_argument(
        "--out",
        default="output/pixel-smoke/captured-events.jsonl",
    )
    capture.set_defaults(func=cmd_capture)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
