"""Read configured DailyHotApi boards and RSS/Atom feeds."""

import calendar
import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from html import unescape
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen
from xml.etree import ElementTree


BOARDS = {
    "weibo": "微博",
    "zhihu": "知乎",
    "douyin": "抖音",
    "toutiao": "今日头条",
    "thepaper": "澎湃新闻",
    "qq-news": "腾讯新闻",
}
OFFICIAL = (
    ("openai", "OpenAI", "https://openai.com/news/rss.xml"),
    ("google-ai", "Google AI", "https://blog.google/technology/ai/rss/"),
    ("openai-status", "OpenAI Status", "https://status.openai.com/history.rss"),
    ("claude-status", "Claude Status", "https://status.claude.com/history.rss"),
    ("deepseek-status", "DeepSeek Status", "https://status.deepseek.com/history.rss"),
    ("kimi-code", "Kimi Code", "https://github.com/MoonshotAI/kimi-code/releases.atom"),
)
TRACKING_PARAMS = {"spm", "igshid", "fbclid", "gclid"}
MAX_RESPONSE = 2_000_000
WEIBO_SNAPSHOT = "https://solarhell.github.io/hot-search-crawler/latest.json"


@dataclass(frozen=True)
class Source:
    id: str
    name: str
    url: str
    format: str
    official: bool = False
    fallback: str | None = None


@dataclass(frozen=True)
class Entry:
    title: str
    url: str
    description: str = ""
    published: int | None = None
    rank: int | None = None
    origin: str | None = None


def default_sources(hot_api, weibo_snapshot=WEIBO_SNAPSHOT):
    return [
        Source(key, name, f"{hot_api.rstrip('/')}/{key}", "dailyhot",
               fallback=weibo_snapshot if key == "weibo" else None)
        for key, name in BOARDS.items()
    ] + [Source(key, name, url, "feed", True) for key, name, url in OFFICIAL]


def clean_text(value, length):
    if not isinstance(value, str):
        return ""
    return " ".join(unescape(re.sub(r"<[^>]*>", " ", value)).split())[:length]


def canonical_url(value):
    if not isinstance(value, str) or len(value) > 2500:
        return None
    try:
        parts = urlsplit(value.strip())
        if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username:
            return None
        params = sorted(
            (key, val) for key, val in parse_qsl(parts.query, keep_blank_values=True)
            if not key.lower().startswith("utm_") and key.lower() not in TRACKING_PARAMS
        )
        return urlunsplit((parts.scheme, parts.netloc.lower(), parts.path or "/", urlencode(params), ""))
    except ValueError:
        return None


def timestamp(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if abs(value) < 10**15 and math.isfinite(value):
            return int(value / 1000 if value > 10_000_000_000 else value)
    if isinstance(value, str):
        for parser in (datetime.fromisoformat, parsedate_to_datetime):
            try:
                date = parser(value.replace("Z", "+00:00"))
                return int(date.timestamp()) if date.tzinfo else calendar.timegm(date.timetuple())
            except (ValueError, TypeError, OverflowError):
                continue
    return None


def dailyhot_entries(payload):
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list) or payload.get("code", 200) != 200:
        raise ValueError("一条有效的热榜响应应包含 data 列表")
    entries = []
    for rank, row in enumerate(rows[:50], 1):
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("title"), 350)
        url = canonical_url(row.get("url")) or canonical_url(row.get("mobileUrl"))
        if title and url:
            entries.append(Entry(
                title, url, clean_text(row.get("desc"), 1000), timestamp(row.get("timestamp")),
                rank, canonical_url(row.get("originalUrl")),
            ))
    if rows and not entries:
        raise ValueError("热榜返回了条目，但没有可读的标题与链接")
    return entries


def weibo_snapshot_entries(payload, now):
    boards = payload.get("boards") if isinstance(payload, dict) else None
    if not isinstance(boards, list):
        raise ValueError("微博备用快照格式无效")
    board = next((row for row in boards if isinstance(row, dict) and row.get("platform") == "weibo"), None)
    if not board or board.get("ok") is not True or not isinstance(board.get("items"), list):
        raise ValueError("微博备用快照无可用榜单")
    fetched = timestamp(board.get("fetched_at") or payload.get("generated_at"))
    if fetched is None or not now - 45 * 60 <= fetched <= now + 600:
        raise ValueError("微博备用快照超过 45 分钟")
    return dailyhot_entries({"data": board["items"]})


def feed_entries(payload, base_url, title_prefix=""):
    root = ElementTree.fromstring(payload)
    if root.tag.rsplit("}", 1)[-1] not in {"rss", "feed", "RDF"}:
        raise ValueError("返回内容不是 RSS 或 Atom")
    entries = []
    rows = [row for row in root.iter() if row.tag.rsplit("}", 1)[-1] in {"item", "entry"}]
    for row in rows[:50]:
        fields = {}
        for field in row:
            name = field.tag.rsplit("}", 1)[-1]
            if name == "link":
                if field.get("rel", "alternate") == "alternate":
                    fields[name] = field.get("href") or field.text
            else:
                fields[name] = "".join(field.itertext())
        title = clean_text(fields.get("title"), 350)
        link = fields.get("link")
        url = canonical_url(urljoin(base_url, link)) if link else None
        if title and url:
            entries.append(Entry(
                f"{title_prefix}{title}", url, clean_text(fields.get("description") or fields.get("summary"), 1000),
                timestamp(fields.get("pubDate") or fields.get("published") or fields.get("updated")),
            ))
    if rows and not entries:
        raise ValueError("订阅源返回了条目，但没有可读的标题与链接")
    return entries


def download(url):
    request = Request(url, headers={"User-Agent": "ledian/0.1 (LAN topic feed)"})
    with urlopen(request, timeout=12) as response:
        data = response.read(MAX_RESPONSE + 1)
        if len(data) > MAX_RESPONSE:
            raise ValueError("响应超过 2 MB")
        return data, response.geturl()


def fetch(source):
    try:
        data, final_url = download(source.url)
        if source.format == "dailyhot":
            return dailyhot_entries(json.loads(data))
        prefix = "Kimi Code · " if source.id == "kimi-code" else ""
        return feed_entries(data, final_url, prefix)
    except (OSError, ValueError) as primary:
        if not source.fallback:
            raise
        try:
            data, _ = download(source.fallback)
            return weibo_snapshot_entries(json.loads(data), int(time.time()))
        except (OSError, ValueError) as fallback:
            raise ValueError(f"主源：{primary}；备用源：{fallback}") from fallback


def collect(store, sources, reader=fetch, changed=None):
    """Save each board independently so one failed source leaves others available."""
    results = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = {pool.submit(reader, source): source for source in sources}
        for job in as_completed(jobs):
            source = jobs[job]
            now = int(time.time())
            try:
                entries = job.result()
                store.ingest(source, entries, now)
                results[source.id] = {"count": len(entries)}
                if changed:
                    changed()
            except (OSError, ValueError, ElementTree.ParseError) as exc:
                error = str(exc)[:220]
                store.source_failed(source, now, error)
                results[source.id] = {"error": error}
    return results
