import json
import os
import threading
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection
from pathlib import Path
from urllib.parse import urlencode

from ledian.__main__ import Server, configured_sources
from ledian.sources import Entry, collect, dailyhot_entries, feed_entries, weibo_snapshot_entries
from ledian.store import TRACK_SECONDS, Store


def storage(test):
    path = Path(os.getenv("LEDIAN_TEST_DATA", "data")) / f"test-{uuid.uuid4().hex}.sqlite3"
    test.addCleanup(path.unlink, missing_ok=True)
    return Store(path)


BOARDS = configured_sources("http://127.0.0.1:6688", None)
WEIBO, ZHIHU = BOARDS[:2]
OPENAI = next(source for source in BOARDS if source.id == "openai")


class SourceTests(unittest.TestCase):
    def test_weibo_fallback_rejects_stale_or_failed_boards(self):
        now = int(time.time())
        board = {"platform": "weibo", "ok": True, "fetched_at": now,
                 "items": [{"title": "真实热榜", "url": "https://weibo.com/item", "rank": 1}]}
        self.assertEqual(weibo_snapshot_entries({"boards": [board]}, now)[0].rank, 1)
        with self.assertRaises(ValueError):
            weibo_snapshot_entries({"boards": [board]}, now + 46 * 60)
        board["ok"] = False
        with self.assertRaises(ValueError):
            weibo_snapshot_entries({"boards": [board]}, now)

    def test_parsers_and_independent_failure(self):
        payload = {"code": 200, "data": [
            {"title": "<em>Claude</em> 新功能", "url": "https://weibo.com/a?utm_source=search&id=7"},
        ]}
        item = dailyhot_entries(payload)[0]
        self.assertEqual(item.title, "Claude 新功能")
        self.assertEqual(item.url, "https://weibo.com/a?id=7")
        self.assertEqual(item.rank, 1)
        feed = b"""<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Test</title>
            <link href="/entry"/><updated>2026-11-24T08:00:00Z</updated></entry></feed>"""
        self.assertEqual(feed_entries(feed, "https://openai.com/rss")[0].url, "https://openai.com/entry")
        store = storage(self)
        store.configure_sources([WEIBO, ZHIHU])
        store.ingest(ZHIHU, [], 100)

        def reader(source):
            if source == ZHIHU:
                raise ValueError("broken board")
            return [item]

        self.assertEqual(collect(store, [WEIBO, ZHIHU], reader)[WEIBO.id]["count"], 1)
        statuses = {row["id"]: row for row in store.source_status()}
        self.assertEqual(statuses[ZHIHU.id]["succeeded_at"], 100)
        self.assertEqual(statuses[ZHIHU.id]["error"], "broken board")
        self.assertIsNotNone(statuses[WEIBO.id]["succeeded_at"])


class BackendTests(unittest.TestCase):
    def setUp(self):
        self.store = storage(self)
        self.store.configure_sources(BOARDS)
        self.now = int(time.time())
        self.server = Server(("127.0.0.1", 0), self.store, b"test-invite")
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.close_server)

    def close_server(self):
        self.server.stop.set()
        self.server.wake()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)

    def call(self, method, route, body=None, cookie=None):
        conn = HTTPConnection("127.0.0.1", self.server.server_port, timeout=10)
        headers = {"Content-Type": "application/json"}
        if cookie:
            headers["Cookie"] = cookie
        encoded = json.dumps(body).encode() if body is not None else None
        try:
            conn.request(method, route, body=encoded, headers=headers)
            response = conn.getresponse()
            session = response.getheader("Set-Cookie", "").split(";")[0]
            return response.status, json.loads(response.read()), session
        finally:
            conn.close()

    def member(self, name):
        secret = {"username": name, "password": "a-long-password"}
        status, registered, _ = self.call("POST", "/api/register", dict(secret, invite="test-invite"))
        self.assertEqual(status, 201)
        status, signed_in, cookie = self.call("POST", "/api/login", secret)
        self.assertEqual(status, 200)
        self.assertEqual(signed_in, registered)
        return cookie, signed_in["id"]

    def events(self, cookie, **query):
        status, result, _ = self.call("GET", "/api/events?" + urlencode(query), cookie=cookie)
        self.assertEqual(status, 200)
        return result["items"]

    def test_registration_session_and_concurrent_first_push(self):
        self.assertEqual(self.call("GET", "/api/health")[0], 200)
        self.assertEqual(self.call("GET", "/api/events")[0], 401)
        self.assertEqual(self.call("POST", "/api/register", {"invite": "wrong"})[0], 403)
        self.assertEqual(self.call("POST", "/api/login", {"username": "unknown", "password": "wrong"})[0], 401)
        alice, _ = self.member("Alice")
        bob, _ = self.member("Bob")
        cara, cara_id = self.member("Cara")
        self.assertEqual(
            self.call("POST", "/api/login", {"username": "alice", "password": "wrong-password"})[0], 401,
        )
        self.store.ingest(WEIBO, [Entry("即梦 视频教程", "https://weibo.com/video", rank=5)], self.now)
        event_id = self.events(alice, q="即梦 视频")[0]["id"]
        self.assertEqual(self.events(alice, scope="saved"), [])
        route = f"/api/events/{event_id}/push"
        with ThreadPoolExecutor(max_workers=3) as pool:
            watching = pool.submit(self.call, "GET", "/api/notifications?wait=5", cookie=cara)
            pushing = [pool.submit(self.call, "POST", route, {}, cookie) for cookie in (alice, bob)]
            self.assertEqual([request.result()[0] for request in pushing], [200, 200])
            notices = watching.result()[1]["items"]
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices[0]["kind"], "push")
        notice = notices[0]["id"]
        self.assertEqual(self.call("POST", route, {}, alice)[1]["push_count"], 2)
        self.assertEqual(len(self.events(alice, scope="saved")), 1)
        self.assertEqual(self.call("POST", f"/api/notifications/{notice}/read", {}, cara)[0], 200)
        self.assertEqual(self.call("GET", "/api/notifications?unread=1", cookie=cara)[1]["items"], [])
        separate = [self.call("GET", "/api/notifications?unread=1", cookie=c)[1] for c in (alice, bob)]
        self.assertEqual(sum(box["unread_count"] for box in separate), 1)
        restarted = Store(self.store.path)
        self.assertEqual(restarted.member_for(cara.split("=", 1)[1], self.now + 1)["id"], cara_id)
        self.assertEqual(restarted.event(event_id, cara_id, self.now + 1)["push_count"], 2)
        self.assertEqual(restarted.inbox(cara_id)["unread_count"], 0)

    def test_official_dedup_filters_and_tracking_cutoff(self):
        user, user_id = self.member("Editor")
        official = Entry("OpenAI 发布 GPT-9 开源模型", "https://openai.com/news/example", published=self.now)
        self.store.ingest(OPENAI, [official], self.now)
        self.store.ingest(OPENAI, [official], self.now + 600)
        event = self.events(user, scope="qualified", category="product")[0]
        self.assertEqual(self.events(user, scope="qualified", category="industry")[0]["id"], event["id"])
        self.assertEqual(len(self.store.inbox(user_id)["items"]), 1)
        self.assertEqual(self.events(user, source="zhihu"), [])
        self.store.push(event["id"], user_id, self.now)
        cross = Entry("OpenAI 发布 GPT-9 开源模型解读", "https://zhihu.com/question/example")
        self.store.ingest(ZHIHU, [cross], self.now + 1200)
        self.assertEqual(len(self.store.event(event["id"], user_id, self.now + 1200)["sources"]), 2)
        late = Entry("OpenAI 发布 GPT-9 开源模型的报道", "https://weibo.com/late")
        self.store.ingest(WEIBO, [late], self.now + TRACK_SECONDS)
        saved = self.store.event(event["id"], user_id, self.now + TRACK_SECONDS)
        self.assertEqual(saved["tracking_status"], "ended")
        self.assertEqual(len(saved["sources"]), 2)
        self.assertEqual(len(self.store.events(
            user_id, self.now + TRACK_SECONDS, "saved", "all", ["gpt-9"], None, None, None, "ended", 20, 0,
        )["items"]), 1)

    def test_independence_rise_stale_official_and_manual_merge(self):
        user, user_id = self.member("Reader")
        url = "https://weibo.com/rising"
        for step, rank in enumerate((15, 12, 9)):
            self.store.ingest(WEIBO, [Entry("Gemini 新功能实测", url, rank=rank)], self.now + 600 * step)
        self.assertEqual(self.events(user, scope="qualified")[0]["qualification"], "rising")
        reprint = "Claude 7 发布新模型"
        other_title = "Claude 7 发布新模型后续报道"
        self.store.ingest(WEIBO, [Entry(reprint, "https://weibo.com/claude")], self.now)
        self.store.ingest(ZHIHU, [Entry(reprint, "https://zhihu.com/claude")], self.now)
        self.assertEqual(self.events(user, scope="qualified", q="Claude"), [])
        paper = next(source for source in BOARDS if source.id == "thepaper")
        self.store.ingest(paper, [Entry(other_title, "https://thepaper.cn/claude")], self.now)
        claude = self.events(user, scope="qualified", q="Claude")[0]
        self.assertEqual(claude["qualification"], "independent")
        self.store.ingest(OPENAI, [
            Entry("OpenAI 发布 GPT-8 模型", "https://openai.com/old", published=self.now - 8 * 86400),
        ], self.now)
        self.assertEqual(self.store.inbox(user_id)["items"], [])
        new_id = self.events(user, q="GPT-8")[0]["id"]
        self.store.push(new_id, user_id, self.now)
        target = self.call("POST", f"/api/events/{claude['id']}/merge", {"event_id": new_id}, user)
        self.assertEqual(target[0], 200)
        self.assertEqual(target[1]["push_count"], 1)
        self.assertEqual(self.call("GET", f"/api/events/{new_id}", cookie=user)[1]["id"], claude["id"])
        self.assertEqual(len(self.events(user, q="GPT-8", scope="saved")), 1)

    def test_search_filters_and_watch_changes_apply_to_existing_events(self):
        user, _ = self.member("Editor")
        self.store.ingest(WEIBO, [
            Entry("即梦 视频实测", "https://weibo.com/one"),
            Entry("Sora 视频实测", "https://weibo.com/two"),
        ], self.now)
        self.assertEqual(len(self.events(user, category="product", q="视频")), 1)
        self.assertEqual(self.call("PUT", "/api/watch", {"Sora": []}, user)[0], 200)
        results = self.events(user, category="product", source="weibo", since=self.now, until=self.now + 1)
        self.assertEqual([event["title"] for event in results], ["Sora 视频实测"])
        self.assertEqual(self.events(user, since=self.now + 1), [])
        self.assertEqual(self.call("GET", "/api/events?limit=0", cookie=user)[0], 400)
        self.assertEqual(self.call("POST", "/api/logout", {}, user)[0], 200)
        self.assertEqual(self.call("GET", "/api/me", cookie=user)[0], 401)

    def test_similar_models_stay_separate_and_failed_run_breaks_rise(self):
        user, _ = self.member("Reader")
        for idx, model in enumerate(("Gemini 9 Flash", "Gemini 9 Pro")):
            self.store.ingest(WEIBO, [
                Entry(f"Google 发布 {model} 模型", f"https://weibo.com/model-{idx}", rank=7),
            ], self.now + idx)
        self.assertEqual(len(self.events(user, q="Gemini")), 2)
        recent = "https://weibo.com/recent"
        self.store.ingest(WEIBO, [Entry("即梦 视频实测", recent, rank=12)], self.now + 2)
        self.store.ingest(WEIBO, [Entry("即梦 视频实测", recent, rank=9)], self.now + 3)
        self.store.source_failed(WEIBO, self.now + 4, "源站不可用")
        self.store.ingest(WEIBO, [Entry("即梦 视频实测", recent, rank=6)], self.now + 5)
        self.assertEqual(self.events(user, scope="qualified", q="即梦"), [])


if __name__ == "__main__":
    unittest.main()
