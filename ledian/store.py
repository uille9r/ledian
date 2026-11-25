"""SQLite storage for members, topic events, and per-member inboxes."""

import hashlib
import hmac
import json
import re
import secrets
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

from .topics import DEFAULT_WATCH, RUMOR, classify, event_key, major_change, normalized, same_event


DAY = 86_400
TRACK_SECONDS = 2 * DAY
SESSION_SECONDS = 7 * DAY
AUTH_WINDOW = 15 * 60
PBKDF2_ROUNDS = 600_000
USER_NAME = re.compile(r"[\w.-]{2,40}")

SCHEMA = """
CREATE TABLE IF NOT EXISTS members (
    id TEXT PRIMARY KEY, username TEXT NOT NULL, name_key TEXT UNIQUE NOT NULL,
    salt BLOB NOT NULL, password_hash BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
    token_hash BLOB PRIMARY KEY, member_id TEXT NOT NULL REFERENCES members(id),
    expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS auth_limits (
    key TEXT PRIMARY KEY, started_at INTEGER NOT NULL, failures INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY, name TEXT NOT NULL, official INTEGER NOT NULL,
    attempted_at INTEGER, succeeded_at INTEGER, error TEXT, run INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, first_seen INTEGER NOT NULL,
    last_seen INTEGER NOT NULL, products TEXT NOT NULL DEFAULT '[]', industry TEXT NOT NULL DEFAULT '[]',
    qualified_at INTEGER, qualification TEXT, pushed_at INTEGER
);
CREATE TABLE IF NOT EXISTS event_aliases (
    old_id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL REFERENCES events(id)
);
CREATE TABLE IF NOT EXISTS entries (
    id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL REFERENCES events(id),
    source_id TEXT NOT NULL REFERENCES sources(id), url TEXT NOT NULL, title TEXT NOT NULL,
    title_key TEXT NOT NULL, description TEXT NOT NULL, origin TEXT NOT NULL, published INTEGER,
    first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL, rank INTEGER, run INTEGER NOT NULL,
    rise INTEGER NOT NULL, UNIQUE(source_id, url)
);
CREATE TABLE IF NOT EXISTS pushes (
    event_id INTEGER NOT NULL REFERENCES events(id), member_id TEXT NOT NULL REFERENCES members(id),
    created_at INTEGER NOT NULL, PRIMARY KEY(event_id, member_id)
);
CREATE TABLE IF NOT EXISTS notices (
    id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER NOT NULL REFERENCES events(id),
    kind TEXT NOT NULL, title TEXT NOT NULL, impact TEXT NOT NULL, url TEXT NOT NULL,
    created_at INTEGER NOT NULL, UNIQUE(event_id, kind)
);
CREATE TABLE IF NOT EXISTS inbox (
    member_id TEXT NOT NULL REFERENCES members(id), notice_id INTEGER NOT NULL REFERENCES notices(id),
    read_at INTEGER, PRIMARY KEY(member_id, notice_id)
);
CREATE INDEX IF NOT EXISTS entries_event ON entries(event_id, first_seen);
CREATE INDEX IF NOT EXISTS entries_title ON entries(title_key);
CREATE INDEX IF NOT EXISTS entries_url ON entries(url);
CREATE INDEX IF NOT EXISTS events_recent ON events(last_seen);
CREATE INDEX IF NOT EXISTS sessions_expiration ON sessions(expires_at);
CREATE INDEX IF NOT EXISTS inbox_notice ON inbox(notice_id);
"""


class Rejected(ValueError):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def password_hash(password, salt):
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ROUNDS)


def token_hash(token):
    return hashlib.sha256(token.encode("utf-8")).digest()


def first(*values):
    return min((value for value in values if value is not None), default=None)


class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.db() as db:
            db.execute("PRAGMA journal_mode = WAL")
            db.executescript(SCHEMA)
            db.execute("INSERT OR IGNORE INTO settings VALUES ('watch', ?)", (json.dumps(DEFAULT_WATCH),))

    @contextmanager
    def db(self, write=False):
        conn = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.create_function("fold", 1, normalized, deterministic=True)
            if write:
                conn.execute("BEGIN IMMEDIATE")
            yield conn
            if write:
                conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def register(self, username, password):
        if not isinstance(username, str) or not USER_NAME.fullmatch(normalized(username.strip())):
            raise Rejected(400, "账号需为 2–40 个字母、数字、下划线、点或连字符")
        if not isinstance(password, str) or not 8 <= len(password) <= 128:
            raise Rejected(400, "密码需为 8–128 个字符")
        username = username.strip()
        member_id, salt = uuid.uuid4().hex, secrets.token_bytes(16)
        digest = password_hash(password, salt)
        try:
            with self.db(write=True) as db:
                db.execute(
                    "INSERT INTO members VALUES (?, ?, ?, ?, ?)",
                    (member_id, username, normalized(username), salt, digest),
                )
        except sqlite3.IntegrityError as exc:
            raise Rejected(409, "账号已存在") from exc
        return {"id": member_id, "username": username}

    def login(self, username, password, now):
        if not isinstance(username, str) or not isinstance(password, str) or len(password) > 128:
            raise Rejected(401, "账号或密码错误")
        with self.db() as db:
            member = db.execute(
                "SELECT * FROM members WHERE name_key = ?", (normalized(username.strip()),),
            ).fetchone()
        salt = member["salt"] if member else bytes(16)
        expected = member["password_hash"] if member else bytes(32)
        if not hmac.compare_digest(password_hash(password, salt), expected) or not member:
            raise Rejected(401, "账号或密码错误")
        token = secrets.token_urlsafe(32)
        with self.db(write=True) as db:
            db.execute("DELETE FROM sessions WHERE expires_at <= ?", (now,))
            db.execute("INSERT INTO sessions VALUES (?, ?, ?)", (token_hash(token), member["id"], now + SESSION_SECONDS))
        return {"id": member["id"], "username": member["username"]}, token

    def allow_auth(self, key, now, limit):
        with self.db(write=True) as db:
            db.execute("DELETE FROM auth_limits WHERE started_at <= ?", (now - AUTH_WINDOW,))
            row = db.execute("SELECT failures FROM auth_limits WHERE key = ?", (key,)).fetchone()
            if row and row["failures"] >= limit:
                raise Rejected(429, "尝试次数过多，请稍后重试")
            db.execute(
                """INSERT INTO auth_limits VALUES (?, ?, 1)
                   ON CONFLICT(key) DO UPDATE SET failures = failures + 1""", (key, now),
            )

    def reset_auth(self, key):
        with self.db(write=True) as db:
            db.execute("DELETE FROM auth_limits WHERE key = ?", (key,))

    def member_for(self, token, now):
        with self.db() as db:
            row = db.execute(
                """SELECT members.id, members.username FROM sessions JOIN members ON members.id = member_id
                   WHERE token_hash = ? AND expires_at > ?""", (token_hash(token), now),
            ).fetchone()
        return dict(row) if row else None

    def logout(self, token):
        with self.db(write=True) as db:
            db.execute("DELETE FROM sessions WHERE token_hash = ?", (token_hash(token),))

    def configure_sources(self, sources):
        with self.db(write=True) as db:
            for source in sources:
                db.execute(
                    """INSERT INTO sources (id, name, official) VALUES (?, ?, ?)
                       ON CONFLICT(id) DO UPDATE SET name = excluded.name, official = excluded.official""",
                    (source.id, source.name, source.official),
                )

    def source_status(self):
        with self.db() as db:
            return [dict(row) for row in db.execute(
                "SELECT id, name, official, attempted_at, succeeded_at, error FROM sources ORDER BY id",
            )]

    def source_failed(self, source, now, error):
        with self.db(write=True) as db:
            db.execute(
                "UPDATE sources SET attempted_at = ?, error = ? WHERE id = ?", (now, error, source.id),
            )

    @staticmethod
    def _watch(db):
        return json.loads(db.execute("SELECT value FROM settings WHERE key = 'watch'").fetchone()[0])

    def watch(self):
        with self.db() as db:
            return self._watch(db)

    def set_watch(self, watch):
        if not isinstance(watch, dict) or len(watch) > 50:
            raise Rejected(400, "关注列表需为产品到别名的映射，最多 50 个产品")
        for name, aliases in watch.items():
            if not name.strip() or len(name) > 48 or not isinstance(aliases, list) or len(aliases) > 20:
                raise Rejected(400, "产品名或别名数量无效")
            if any(not isinstance(alias, str) or not alias.strip() or len(alias) > 48 for alias in aliases):
                raise Rejected(400, "别名需为 1–48 个字符")
        watch = {name.strip(): [alias.strip() for alias in aliases] for name, aliases in watch.items()}
        with self.db(write=True) as db:
            db.execute("UPDATE settings SET value = ? WHERE key = 'watch'", (json.dumps(watch),))
            for row in db.execute("SELECT id FROM events").fetchall():
                self._categorize(db, row["id"], watch)
        return watch

    @staticmethod
    def _categorize(db, event_id, watch):
        products, industry = set(), set()
        for row in db.execute("SELECT title, description FROM entries WHERE event_id = ?", (event_id,)):
            names, categories = classify(row["title"], row["description"], watch)
            products.update(names)
            industry.update(categories)
        db.execute(
            "UPDATE events SET products = ?, industry = ? WHERE id = ?",
            (json.dumps(sorted(products)), json.dumps(sorted(industry)), event_id),
        )
        return sorted(products), sorted(industry)

    @staticmethod
    def _notify(db, event_id, kind, title, impact, url, now, actor=None):
        notice = db.execute(
            """INSERT INTO notices(event_id, kind, title, impact, url, created_at) VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(event_id, kind) DO NOTHING RETURNING id""",
            (event_id, kind, title, impact, url, now),
        ).fetchone()
        if notice:
            db.execute(
                "INSERT INTO inbox(member_id, notice_id) SELECT id, ? FROM members WHERE id != ?",
                (notice["id"], actor or ""),
            )

    @staticmethod
    def _matching_event(db, entry, now):
        duplicate = db.execute(
            """SELECT e.id FROM entries x JOIN events e ON e.id = x.event_id
               WHERE x.url = ? AND (e.pushed_at IS NULL OR e.pushed_at > ?) ORDER BY e.id LIMIT 1""",
            (entry.url, now - TRACK_SECONDS),
        ).fetchone()
        if duplicate:
            return duplicate["id"]
        candidates = db.execute(
            """SELECT e.id, x.title FROM events e JOIN entries x ON e.id = x.event_id
               WHERE (e.pushed_at > ? OR (e.pushed_at IS NULL AND e.last_seen > ?))
               ORDER BY (x.title_key = ?) DESC, e.pushed_at IS NOT NULL DESC, e.last_seen DESC LIMIT 1000""",
            (now - TRACK_SECONDS, now - 3 * DAY, event_key(entry.title)),
        )
        matches = {row["id"] for row in candidates if same_event(entry.title, row["title"])}
        return matches.pop() if len(matches) == 1 else None

    @staticmethod
    def _qualify(db, event_id, source, entry, products, industry, now):
        event = db.execute("SELECT qualified_at, pushed_at FROM events WHERE id = ?", (event_id,)).fetchone()
        if event["pushed_at"] is not None and now >= event["pushed_at"] + TRACK_SECONDS:
            return
        if not (products or industry):
            return
        official_host = urlsplit(source.url).hostname
        official_link = urlsplit(entry.url).hostname
        action = major_change(entry.title, products, industry)
        if source.official and official_link == official_host and action and (
            entry.published is not None and now - TRACK_SECONDS <= entry.published <= now + 600
        ):
            db.execute(
                """UPDATE events SET qualified_at = COALESCE(qualified_at, ?), qualification = 'official'
                   WHERE id = ?""", (now, event_id),
            )
            affected = "、".join(products or industry)
            Store._notify(db, event_id, "official", entry.title, f"{affected}：{action}", entry.url, now)
            return
        if event["qualified_at"] is not None or RUMOR.search(entry.title):
            return
        reports = [row for row in db.execute(
            "SELECT * FROM entries WHERE event_id = ? AND last_seen >= ?", (event_id, now - 3 * DAY),
        ) if not RUMOR.search(row["title"])]
        independent = any(
            left["source_id"] != right["source_id"] and left["origin"] != right["origin"]
            and left["title_key"] != right["title_key"]
            for index, left in enumerate(reports) for right in reports[index + 1:]
        )
        reason = "independent" if independent else "rising" if any(row["rise"] >= 2 for row in reports) else None
        if reason:
            db.execute(
                "UPDATE events SET qualified_at = ?, qualification = ? WHERE id = ?", (now, reason, event_id),
            )

    def ingest(self, source, entries, now):
        with self.db(write=True) as db:
            state = db.execute("SELECT run, error FROM sources WHERE id = ?", (source.id,)).fetchone()
            run = state["run"] + 1
            watch = self._watch(db)
            seen = set()
            for entry in entries:
                if entry.url in seen:
                    continue
                seen.add(entry.url)
                prior = db.execute(
                    "SELECT * FROM entries WHERE source_id = ? AND url = ?", (source.id, entry.url),
                ).fetchone()
                if prior:
                    event_id = prior["event_id"]
                    rise = prior["rise"] + 1 if (
                        not state["error"] and prior["run"] == run - 1
                        and entry.rank is not None and prior["rank"] is not None
                        and prior["rank"] - entry.rank >= 3
                    ) else 0
                else:
                    event_id, rise = self._matching_event(db, entry, now), 0
                    if event_id is None:
                        event_id = db.execute(
                            "INSERT INTO events(title, first_seen, last_seen) VALUES (?, ?, ?)",
                            (entry.title, now, now),
                        ).lastrowid
                origin = urlsplit(entry.origin or entry.url).hostname or entry.url
                db.execute(
                    """INSERT INTO entries(event_id, source_id, url, title, title_key, description, origin,
                            published, first_seen, last_seen, rank, run, rise) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(source_id, url) DO UPDATE SET last_seen = excluded.last_seen,
                           rank = excluded.rank, run = excluded.run, rise = excluded.rise,
                           title = excluded.title, title_key = excluded.title_key, description = excluded.description,
                           origin = excluded.origin, published = COALESCE(excluded.published, entries.published)""",
                    (event_id, source.id, entry.url, entry.title, event_key(entry.title), entry.description,
                     origin.lower().removeprefix("www."), entry.published, now, now, entry.rank, run, rise),
                )
                db.execute("UPDATE events SET last_seen = ? WHERE id = ?", (now, event_id))
                products, industry = self._categorize(db, event_id, watch)
                self._qualify(db, event_id, source, entry, products, industry, now)
            db.execute(
                "UPDATE sources SET attempted_at = ?, succeeded_at = ?, error = NULL, run = ? WHERE id = ?",
                (now, now, run, source.id),
            )

    @staticmethod
    def _event(db, event_id):
        row = db.execute(
            "SELECT * FROM events WHERE id = COALESCE((SELECT event_id FROM event_aliases WHERE old_id = ?), ?)",
            (event_id, event_id),
        ).fetchone()
        if not row:
            raise Rejected(404, "找不到热点")
        return row

    @staticmethod
    def _card(db, event, member_id, now):
        card = dict(event)
        event_id, pushed_at = card["id"], card["pushed_at"]
        card["products"] = json.loads(card["products"])
        card["industry"] = json.loads(card["industry"])
        card["tracking_until"] = pushed_at + TRACK_SECONDS if pushed_at is not None else None
        card["tracking_status"] = (
            None if pushed_at is None else "active" if now < card["tracking_until"] else "ended"
        )
        card["sources"] = [dict(row) for row in db.execute(
            """SELECT x.source_id, s.name, s.official, x.title, x.url, x.published,
               x.first_seen, x.last_seen, x.rank FROM entries x JOIN sources s ON s.id = x.source_id
               WHERE x.event_id = ? ORDER BY x.first_seen DESC, x.id DESC""", (event_id,),
        )]
        pushes = db.execute(
            "SELECT COUNT(*) AS total, MAX(member_id = ?) AS mine FROM pushes WHERE event_id = ?",
            (member_id, event_id),
        ).fetchone()
        card["push_count"], card["pushed_by_me"] = pushes["total"], bool(pushes["mine"])
        return card

    def events(self, member_id, now, scope, category, terms, source, since, until, tracking, limit, offset):
        conditions, args = [], []
        if scope == "saved":
            conditions.append("e.pushed_at IS NOT NULL")
        if scope == "qualified":
            conditions.append("e.qualified_at IS NOT NULL")
        time_field = {"saved": "pushed_at", "qualified": "qualified_at", "recent": "last_seen"}[scope]
        if since is not None:
            conditions.append(f"e.{time_field} >= ?")
            args.append(since)
        if until is not None:
            conditions.append(f"e.{time_field} < ?")
            args.append(until)
        if category != "all":
            field = "products" if category == "product" else "industry"
            conditions.append(f"e.{field} != '[]'")
        if tracking:
            operator = ">" if tracking == "active" else "<="
            conditions.append(f"e.pushed_at {operator} ?")
            args.append(now - TRACK_SECONDS)
        if source:
            conditions.append("EXISTS (SELECT 1 FROM entries WHERE event_id = e.id AND source_id = ?)")
            args.append(source)
        for word in terms:
            conditions.append(
                """(instr(fold(e.title || ' ' || e.products || ' ' || e.industry), ?) > 0
                   OR EXISTS (SELECT 1 FROM entries WHERE event_id = e.id
                       AND instr(fold(title || ' ' || description), ?) > 0))""",
            )
            args.extend([word, word])
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        with self.db() as db:
            rows = db.execute(
                f"""SELECT e.* FROM events e {where}
                    ORDER BY e.{time_field} DESC, e.id DESC LIMIT ? OFFSET ?""", (*args, limit + 1, offset),
            ).fetchall()
            return {
                "items": [self._card(db, row, member_id, now) for row in rows[:limit]],
                "next_offset": offset + limit if len(rows) > limit else None,
            }

    def event(self, event_id, member_id, now):
        with self.db() as db:
            return self._card(db, self._event(db, event_id), member_id, now)

    def push(self, event_id, member_id, now):
        with self.db(write=True) as db:
            event = self._event(db, event_id)
            event_id = event["id"]
            db.execute("INSERT OR IGNORE INTO pushes VALUES (?, ?, ?)", (event_id, member_id, now))
            if event["pushed_at"] is None:
                db.execute("UPDATE events SET pushed_at = ? WHERE id = ?", (now, event_id))
                entry = db.execute(
                    "SELECT url FROM entries WHERE event_id = ? ORDER BY first_seen, id LIMIT 1", (event_id,),
                ).fetchone()
                self._notify(db, event_id, "push", event["title"], "团队首次推荐，开始 48 小时追踪",
                             entry["url"], now, actor=member_id)
            return self._card(db, self._event(db, event_id), member_id, now)

    def merge(self, target_id, other_id, member_id, now):
        with self.db(write=True) as db:
            target = self._event(db, target_id)
            other = self._event(db, other_id)
            target_id, other_id = target["id"], other["id"]
            if target_id == other_id:
                return self._card(db, target, member_id, now)
            db.execute("UPDATE entries SET event_id = ? WHERE event_id = ?", (target_id, other_id))
            db.execute(
                """INSERT INTO pushes SELECT ?, member_id, created_at FROM pushes WHERE event_id = ?
                   ON CONFLICT(event_id, member_id) DO UPDATE SET created_at = MIN(created_at, excluded.created_at)""",
                (target_id, other_id),
            )
            db.execute("DELETE FROM pushes WHERE event_id = ?", (other_id,))
            for notice in db.execute("SELECT id, kind FROM notices WHERE event_id = ?", (other_id,)).fetchall():
                twin = db.execute(
                    "SELECT id FROM notices WHERE event_id = ? AND kind = ?", (target_id, notice["kind"]),
                ).fetchone()
                if not twin:
                    db.execute("UPDATE notices SET event_id = ? WHERE id = ?", (target_id, notice["id"]))
                    continue
                db.execute(
                    """INSERT INTO inbox SELECT member_id, ?, read_at FROM inbox WHERE notice_id = ?
                       ON CONFLICT(member_id, notice_id) DO UPDATE SET
                       read_at = COALESCE(inbox.read_at, excluded.read_at)""", (twin["id"], notice["id"]),
                )
                db.execute("DELETE FROM inbox WHERE notice_id = ?", (notice["id"],))
                db.execute("DELETE FROM notices WHERE id = ?", (notice["id"],))
            db.execute("UPDATE event_aliases SET event_id = ? WHERE event_id = ?", (target_id, other_id))
            db.execute("INSERT INTO event_aliases VALUES (?, ?)", (other_id, target_id))
            first_qualified = first(target["qualified_at"], other["qualified_at"])
            reason = "official" if "official" in (target["qualification"], other["qualification"]) else (
                target["qualification"] if target["qualified_at"] == first_qualified else other["qualification"]
            )
            db.execute(
                """UPDATE events SET first_seen = ?, last_seen = ?, pushed_at = ?, qualified_at = ?, qualification = ?
                   WHERE id = ?""",
                (min(target["first_seen"], other["first_seen"]), max(target["last_seen"], other["last_seen"]),
                 first(target["pushed_at"], other["pushed_at"]), first_qualified, reason, target_id),
            )
            db.execute("DELETE FROM events WHERE id = ?", (other_id,))
            self._categorize(db, target_id, self._watch(db))
            return self._card(db, self._event(db, target_id), member_id, now)

    def inbox(self, member_id, after=0, unread=False):
        with self.db() as db:
            rows = db.execute(
                """SELECT n.*, i.read_at FROM inbox i JOIN notices n ON n.id = i.notice_id
                   WHERE i.member_id = ? AND n.id > ? AND (? = 0 OR i.read_at IS NULL)
                   ORDER BY n.id ASC LIMIT 100""", (member_id, after, unread),
            ).fetchall()
            unread_count = db.execute(
                "SELECT COUNT(*) FROM inbox WHERE member_id = ? AND read_at IS NULL", (member_id,),
            ).fetchone()[0]
        return {"items": [dict(row) for row in rows], "unread_count": unread_count}

    def mark_read(self, member_id, notice_id, now):
        with self.db(write=True) as db:
            count = db.execute(
                "UPDATE inbox SET read_at = COALESCE(read_at, ?) WHERE member_id = ? AND notice_id = ?",
                (now, member_id, notice_id),
            ).rowcount
            if not count:
                raise Rejected(404, "找不到通知")
