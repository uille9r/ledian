"""Run the ledian API and its feed collector."""

import argparse
import hmac
import json
import logging
import os
import re
import sqlite3
import threading
import time
from http import cookies
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .sources import WEIBO_SNAPSHOT, Source, canonical_url, collect, default_sources, timestamp
from .store import DAY, SESSION_SECONDS, Rejected, Store
from .topics import normalized


LOG = logging.getLogger("ledian")
MAX_BODY = 24_000
INTERVAL = 600
COOKIE = "ledian_session"


def configured_sources(hot_api, extra_file, weibo_snapshot=WEIBO_SNAPSHOT):
    sources = default_sources(hot_api, weibo_snapshot)
    if extra_file:
        rows = json.loads(Path(extra_file).read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            raise ValueError("订阅源配置应为 JSON 列表")
        for row in rows:
            if not isinstance(row, dict) or not {"id", "name", "url"} <= row.keys():
                raise ValueError("订阅源必须提供 id、name 和 url")
            sources.append(Source(row["id"], row["name"], row["url"], row.get("format", "feed"),
                                  row.get("official") is True))
    seen = set()
    for source in sources:
        if not isinstance(source.id, str) or not re.fullmatch(r"[a-z0-9-]{1,40}", source.id):
            raise ValueError("订阅源 ID 格式错误")
        if source.id in seen or source.format not in {"dailyhot", "feed"} or not canonical_url(source.url):
            raise ValueError(f"订阅源 ID 重复、格式不受支持或网址无效：{source.id}")
        if source.fallback and not canonical_url(source.fallback):
            raise ValueError(f"备用源网址无效：{source.id}")
        if not isinstance(source.name, str) or not source.name.strip() or len(source.name) > 80:
            raise ValueError(f"订阅源名称无效：{source.id}")
        seen.add(source.id)
    return sources


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 48

    def __init__(self, address, store, invite, secure_cookie=False):
        super().__init__(address, Handler)
        self.store, self.invite, self.secure_cookie = store, invite, secure_cookie
        self.changed = threading.Condition()
        self.stop = threading.Event()
        self.wait_slots = threading.BoundedSemaphore(16)
        self.request_slots = threading.BoundedSemaphore(48)

    def process_request(self, request, client_address):
        if not self.request_slots.acquire(blocking=False):
            request.settimeout(1)
            try:
                request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
            except OSError:
                pass
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.request_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.request_slots.release()

    def wake(self):
        with self.changed:
            self.changed.notify_all()

    def collect_loop(self, sources):
        while not self.stop.is_set():
            started = time.monotonic()
            try:
                report = collect(self.store, sources, changed=self.wake)
                for source_id, outcome in report.items():
                    if "error" in outcome:
                        LOG.warning("%s 采集失败：%s", source_id, outcome["error"])
            except Exception:
                LOG.exception("采集轮次失败")
            self.stop.wait(max(1, INTERVAL - (time.monotonic() - started)))


def option(params, key, allowed, default):
    value = params.get(key, default)
    if value not in allowed:
        raise Rejected(400, f"{key} 的取值无效")
    return value


def integer(params, key, default, maximum):
    try:
        value = int(params.get(key, default))
    except (ValueError, TypeError) as exc:
        raise Rejected(400, f"{key} 必须为整数") from exc
    if not 0 <= value <= maximum:
        raise Rejected(400, f"{key} 超出范围")
    return value


def time_param(params, key, default=None):
    if key not in params:
        return default
    raw = params[key]
    value = timestamp(int(raw) if raw.isdigit() and len(raw) < 15 else raw)
    if value is None:
        raise Rejected(400, f"{key} 必须为 Unix 秒或 ISO 日期时间")
    return value


class Handler(BaseHTTPRequestHandler):
    server: Server

    def setup(self):
        self.request.settimeout(25)
        super().setup()

    def do_GET(self):
        self.dispatch("GET")

    def do_POST(self):
        self.dispatch("POST")

    def do_PUT(self):
        self.dispatch("PUT")

    def dispatch(self, method):
        try:
            result, code, cookie = self.route(method)
        except Rejected as exc:
            result, code, cookie = {"error": str(exc)}, exc.status, None
        except (BrokenPipeError, ConnectionResetError):
            return
        except (sqlite3.Error, OSError):
            LOG.exception("请求处理失败")
            result, code, cookie = {"error": "服务器暂时无法处理请求"}, 500, None
        try:
            self.respond(code, result, cookie)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def respond(self, status, result, cookie):
        raw = json.dumps(result, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if cookie is not None:
            age = SESSION_SECONDS if cookie else 0
            secure = "; Secure" if self.server.secure_cookie else ""
            self.send_header("Set-Cookie", f"{COOKIE}={cookie}; Path=/; HttpOnly; SameSite=Strict; Max-Age={age}{secure}")
        self.end_headers()
        self.wfile.write(raw)

    def body(self):
        if self.headers.get_content_type() != "application/json":
            raise Rejected(415, "请求正文须使用 application/json")
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= MAX_BODY:
                raise Rejected(413, "请求正文为空或过大")
            data = json.loads(self.rfile.read(length))
            json.dumps(data, ensure_ascii=False).encode("utf-8")
        except Rejected:
            raise
        except (ValueError, TimeoutError, RecursionError) as exc:
            raise Rejected(400, "JSON 正文无效") from exc
        if not isinstance(data, dict):
            raise Rejected(400, "JSON 正文须为对象")
        return data

    def session(self):
        jar = cookies.SimpleCookie()
        try:
            jar.load(self.headers.get("Cookie", ""))
        except cookies.CookieError:
            return ""
        token = jar.get(COOKIE)
        return token.value if token and re.fullmatch(r"[\w-]{43}", token.value, re.ASCII) else ""

    def route(self, method):
        try:
            parts = urlsplit(self.path)
            params = {k: values[-1] for k, values in parse_qs(
                parts.query, keep_blank_values=True, max_num_fields=30,
            ).items()}
        except ValueError as exc:
            raise Rejected(400, "请求路径或查询参数无效") from exc
        path = parts.path
        now = int(time.time())
        store = self.server.store
        if method == "GET" and path in {"/", "/api/health"}:
            return {"ok": True, "service": "ledian"}, 200, None
        if method == "POST" and path in {"/api/register", "/api/login"}:
            data = self.body()
            address = self.client_address[0]
            username, password = data.get("username"), data.get("password")
            if path == "/api/register":
                store.allow_auth(f"register:{address}", now, 12)
                invite = data.get("invite")
                if not isinstance(invite, str) or not hmac.compare_digest(
                    invite.encode("utf-8"), self.server.invite,
                ):
                    raise Rejected(403, "团队邀请码错误")
                return store.register(username, password), 201, None
            store.allow_auth(f"login:{address}", now, 40)
            account_key = None
            if isinstance(username, str):
                account_key = f"account:{address}:{normalized(username.strip())[:40]}"
                store.allow_auth(account_key, now, 12)
            member, token = store.login(username, password, now)
            if account_key:
                store.reset_auth(account_key)
            return member, 200, token
        token = self.session()
        member = store.member_for(token, now) if token else None
        if not member:
            raise Rejected(401, "请先登录")
        member_id = member["id"]
        if method == "POST" and path == "/api/logout":
            self.body()
            store.logout(token)
            self.server.wake()
            return {"ok": True}, 200, ""
        if method == "GET" and path == "/api/me":
            return member, 200, None
        if method == "GET" and path == "/api/sources":
            return {"items": store.source_status()}, 200, None
        if path == "/api/watch":
            if method == "GET":
                return store.watch(), 200, None
            if method == "PUT":
                return store.set_watch(self.body()), 200, None
        if path == "/api/events" and method == "GET":
            return self.list_events(params, member_id, now), 200, None
        event_match = re.fullmatch(r"/api/events/([1-9][0-9]{0,17})(?:/(push|merge))?", path)
        if event_match:
            event_id, action = int(event_match[1]), event_match[2]
            if method == "GET" and action is None:
                return store.event(event_id, member_id, now), 200, None
            if method == "POST" and action in {"push", "merge"}:
                body = self.body()
                if action == "push":
                    card = store.push(event_id, member_id, now)
                else:
                    other_id = body.get("event_id")
                    if type(other_id) is not int or not 0 < other_id < 10**18:
                        raise Rejected(400, "event_id 必须为有效热点编号")
                    card = store.merge(event_id, other_id, member_id, now)
                self.server.wake()
                return card, 200, None
        if method == "GET" and path == "/api/notifications":
            return self.notifications(params, token, member_id), 200, None
        notice_match = re.fullmatch(r"/api/notifications/([1-9][0-9]{0,17})/read", path)
        if notice_match and method == "POST":
            self.body()
            store.mark_read(member_id, int(notice_match[1]), now)
            return {"ok": True}, 200, None
        raise Rejected(404, "找不到此接口")

    def list_events(self, params, member_id, now):
        scope = option(params, "scope", {"recent", "qualified", "saved"}, "recent")
        category = option(params, "category", {"all", "product", "industry"}, "all")
        tracking = option(params, "tracking", {None, "active", "ended"}, None)
        query = params.get("q", "")
        if len(query) > 160:
            raise Rejected(400, "搜索关键词不能超过 160 个字符")
        limit = integer(params, "limit", 20, 100)
        if not limit:
            raise Rejected(400, "limit 必须大于 0")
        since = time_param(params, "since", None if scope == "saved" else now - 7 * DAY)
        until = time_param(params, "until")
        if since is not None and until is not None and until <= since:
            raise Rejected(400, "until 必须晚于 since")
        return self.server.store.events(
            member_id, now, scope, category, normalized(query).split(), params.get("source"),
            since, until, tracking, limit, integer(params, "offset", 0, 1_000_000),
        )

    def notifications(self, params, token, member_id):
        after = integer(params, "after", 0, 10**18)
        wait = integer(params, "wait", 0, 20)
        unread = option(params, "unread", {"0", "1"}, "0") == "1"
        store = self.server.store
        if not wait or not self.server.wait_slots.acquire(blocking=False):
            return store.inbox(member_id, after, unread)
        try:
            deadline = time.monotonic() + wait
            with self.server.changed:
                while True:
                    if not store.member_for(token, int(time.time())):
                        raise Rejected(401, "登录已过期")
                    inbox = store.inbox(member_id, after, unread)
                    remaining = deadline - time.monotonic()
                    if inbox["items"] or remaining <= 0 or self.server.stop.is_set():
                        return inbox
                    self.server.changed.wait(remaining)
        finally:
            self.server.wait_slots.release()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.getenv("LEDIAN_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("LEDIAN_PORT", "3000")))
    parser.add_argument("--db", default=os.getenv("LEDIAN_DB", "data/ledian.sqlite3"))
    parser.add_argument("--hot-api", default=os.getenv("LEDIAN_HOT_API", "http://127.0.0.1:6688"))
    parser.add_argument("--weibo-snapshot", default=os.getenv("LEDIAN_WEIBO_SNAPSHOT", WEIBO_SNAPSHOT))
    parser.add_argument("--sources-file", default=os.getenv("LEDIAN_SOURCES_FILE"))
    parser.add_argument("--secure-cookie", action="store_true")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--collect-once", action="store_true")
    mode.add_argument("--no-collect", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(format="%(levelname)s %(message)s", level=logging.INFO)
    invite = os.getenv("LEDIAN_INVITE_CODE", "")
    if not args.collect_once and not 8 <= len(invite) <= 128:
        parser.error("请先设置 8–128 字符的 LEDIAN_INVITE_CODE")
    try:
        sources = configured_sources(args.hot_api, args.sources_file, args.weibo_snapshot)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    store = Store(args.db)
    store.configure_sources(sources)
    if args.collect_once:
        results = collect(store, sources)
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return int(any("error" in result for result in results.values()))
    with Server((args.host, args.port), store, invite.encode("utf-8"), args.secure_cookie) as server:
        if not args.no_collect:
            worker = threading.Thread(target=server.collect_loop, args=(sources,), daemon=True)
            worker.start()
        LOG.info("ledian API: http://%s:%s", *server.server_address[:2])
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.stop.set()
            server.wake()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
