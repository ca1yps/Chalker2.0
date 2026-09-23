import os
import re
import threading
import time
import uuid
import json
import base64
from typing import Optional, List

import boto3
from botocore.client import Config as _BotoConfig
import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor

from fastapi import FastAPI, Form, File, UploadFile, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Database connection (CockroachDB -- Postgres wire-compatible, via psycopg2)
# ---------------------------------------------------------------------------
# MUHIM: parol/manzil endi FAQAT environment variable orqali beriladi.
# Kodga hech qachon haqiqiy DB parolini yozib qo'ymang (git/GitHub'ga
# tushib qolishi mumkin -- oldingi versiyada aynan shu sodir bo'lgan edi).
# Render -> Environment bo'limiga DATABASE_URL ni qo'shing, masalan:
#   postgresql://<user>:<password>@<host>:26257/<db>?sslmode=verify-full
#
# sslmode=verify-full needs the cluster's CA certificate on disk (see the
# "root.crt" explanation in chat) -- psycopg2/libpq looks for it at:
#   Windows: %APPDATA%\postgresql\root.crt
#   Linux/Render: ~/.postgresql/root.crt
# If that file isn't present, every connection attempt fails with an SSL
# verification error, not a credentials error.

# Render'da "DATABASE_URL" muhit o'zgaruvchisi avtomatik olinadi,
# Local testda esa zaxiradagi CockroachDB manzili ishlatiladi.
DATABASE_URL = os.getenv("DATABASE_URL") or "postgresql://ilyosbek:CAIsL_qC1EfkDeRKwyN98Q@chalkerdb-19950.jxf.gcp-europe-west3.cockroachlabs.cloud:26257/defaultdb?sslmode=require"
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable topilmadi. "
        "Render'da Environment bo'limiga CockroachDB connection string'ini qo'shing."
    )

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_INDEX_CANDIDATES = [
    os.path.join(_BASE_DIR, "templates", "index.html"),
    os.path.join(_BASE_DIR, "index.html"),
]
INDEX = next((p for p in _INDEX_CANDIDATES if os.path.exists(p)), _INDEX_CANDIDATES[-1])

# ---------------------------------------------------------------------------
# JavaScript's Number type only represents integers exactly up to 2^53-1.
# This DB's older tables (posts, comments, likes, ...) still use CockroachDB's
# unique_rowid() ids, which are 64-bit and almost always bigger than that.
# When such an id travels to the browser as a plain JSON number, the browser
# silently rounds it -- so the id the frontend later sends back for a
# like/comment/delete is no longer the real id, and the like either attaches
# to the wrong row or is rejected. Fix: send any such big integer as a JSON
# *string* instead, which JavaScript keeps 100% exact. FastAPI/pydantic
# already accept numeric strings for `int` fields on the way back in, so
# nothing else about the API has to change -- BUT the frontend HTML/JS must
# also wrap these ids in quotes wherever it embeds them into onclick="..."
# handlers, otherwise the browser re-parses them as a numeric literal and
# loses precision a second time at click-time. Both sides need this.
# ---------------------------------------------------------------------------
_JS_MAX_SAFE_INT = 9007199254740991  # 2^53 - 1

def _bigint_safe(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and (value > _JS_MAX_SAFE_INT or value < -_JS_MAX_SAFE_INT):
        return str(value)
    if isinstance(value, dict):
        return {k: _bigint_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_bigint_safe(v) for v in value]
    return value

class BigIntSafeJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return json.dumps(
            _bigint_safe(content),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")

app = FastAPI(title="Chalker", default_response_class=BigIntSafeJSONResponse)
# allow_credentials=True bilan allow_origins=["*"] birga ishlatilishi brauzer
# standartlariga zid (va kerak ham emas -- frontend cookie emas, user_id
# orqali ishlaydi), shuning uchun False qilindi.
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"])


@app.exception_handler(psycopg2.IntegrityError)
def _integrity_error_handler(request, exc):
    """A foreign key / unique / not-null violation almost always means the
    frontend acted on stale data -- liking/commenting on a post or user
    that was already deleted, a double-click race creating a duplicate,
    etc -- not a real server bug. Surface it as a normal 409 with a
    friendly message instead of a raw 500 traceback."""
    return BigIntSafeJSONResponse(
        {"error": "Bu amalni bajarib bo'lmadi: bog'liq ma'lumot topilmadi yoki avval o'chirilgan. Sahifani yangilab qayta urinib ko'ring."},
        status_code=409,
    )


@app.exception_handler(Exception)
async def _global_error_handler(request: Request, exc: Exception):
    # Har qanday BOSHQA kutilmagan xato (masalan: DB ulanishi vaqtincha
    # uzilishi, CockroachDB klasteri "uyquda" bo'lishi, connection pool band
    # bo'lib qolishi) endi umumiy "Server error (500)" o'rniga ANIQ matn
    # bilan qaytadi, va butun ilova qulab tushmaydi. psycopg2.IntegrityError
    # yuqoridagi maxsus handler orqali allaqachon ushlanadi, bu yerga
    # kelmaydi.
    print(f"[UNHANDLED ERROR] {request.method} {request.url.path} -> {type(exc).__name__}: {exc}")
    return BigIntSafeJSONResponse({"error": f"Kutilmagan server xatosi: {exc}"}, status_code=500)


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------
# psycopg2.pool.ThreadedConnectionPool keeps up to POOL_SIZE live connections
# around and reused across requests instead of opening (and leaking, if an
# endpoint errors before closing it) a brand new physical connection to
# CockroachDB on every single API call.
# ---------------------------------------------------------------------------
_POOL_SIZE = 5
_pool_lock = threading.Lock()


def _create_pool_with_retry(retries=10, delay=5):
    """Used only at startup. Right after a deploy/restart, CockroachDB (or
    the network path to it) can briefly refuse new connections -- retry with
    a delay instead of crashing the whole app on deploy. This does NOT help
    if the real cause is bad credentials or a firewall/allowlist blocking
    Render's IP (every attempt will fail the same way and this still raises
    once retries are exhausted)."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.pool.ThreadedConnectionPool(1, _POOL_SIZE, DATABASE_URL)
        except psycopg2.Error as e:
            last_err = e
            if attempt < retries:
                time.sleep(delay)
    raise last_err


_pool = _create_pool_with_retry()


class Conn:
    """Thin wrapper so the rest of the file can keep using the same
    sqlite3-style pattern: c.execute(sql, params).fetchone()/.fetchall(),
    c.commit(), c.close() -- but talking to CockroachDB underneath via
    psycopg2, using a small pool of reused connections. RealDictCursor makes
    fetchone()/fetchall() return plain dict-like rows (r["colname"],
    dict(r), etc), same as the rest of this file expects. Postgres/
    CockroachDB uses '%s' placeholders instead of pyodbc's '?', so every
    query string below was rewritten for that -- the calling code
    (c.execute(sql, params)) didn't change."""

    def __init__(self):
        self._conn = _pool.getconn()
        self._conn.autocommit = False
        self._returned = False
        self._broken = False

    def execute(self, sql, params=()):
        try:
            cur = self._conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(sql, tuple(params))
            return cur
        except psycopg2.IntegrityError:
            # A foreign key / unique / not-null violation (e.g. liking a
            # post that no longer exists, a duplicate follow race, ...).
            # This is a data problem, not a broken connection -- retrying
            # the exact same statement would just fail the same way again,
            # so roll back to keep the connection usable and let the caller
            # (or the global handler below) turn it into a clean response
            # instead of a raw 500.
            try:
                self._conn.rollback()
            except Exception:
                pass
            raise
        except psycopg2.Error:
            # The pooled connection is stale/aborted -- CockroachDB (or a
            # network blip in between) broke it, so the next query on it
            # fails. Roll back and retry once on the same connection instead
            # of bubbling up a 500 for something a reset fixes.
            try:
                self._conn.rollback()
            except Exception:
                pass
            try:
                cur = self._conn.cursor(cursor_factory=RealDictCursor)
                cur.execute(sql, tuple(params))
                return cur
            except Exception:
                # Retry also failed -- this is a real error (bad SQL, DB
                # down, etc), not just a stale connection. Mark the
                # connection broken so it isn't handed back to the pool.
                self._broken = True
                raise

    def commit(self):
        self._conn.commit()

    def close(self):
        # Return the connection to the pool instead of physically closing
        # it, so it can be reused by the next request.
        if self._returned:
            return
        self._returned = True
        if self._broken:
            try:
                _pool.putconn(self._conn, close=True)
            except Exception:
                pass
            return
        try:
            self._conn.rollback()
        except Exception:
            pass
        try:
            _pool.putconn(self._conn)
        except Exception:
            pass

    def __del__(self):
        # Safety net: if an endpoint throws before calling c.close() (a bug,
        # an unexpected error, etc.) the connection still gets returned to
        # the pool here once the Conn object is garbage-collected, instead
        # of being leaked forever and slowly exhausting CockroachDB's
        # connection quota.
        try:
            self.close()
        except Exception:
            pass


def db():
    return Conn()


def init():
    c = db()
    statements = [
        """CREATE TABLE IF NOT EXISTS users(
          id SERIAL PRIMARY KEY,
          username VARCHAR(255) UNIQUE NOT NULL,
          fullname VARCHAR(255),
          school_class VARCHAR(255),
          school_name VARCHAR(255),
          country VARCHAR(255),
          region VARCHAR(255),
          district VARCHAR(255),
          role VARCHAR(50) DEFAULT 'student',
          birth_date VARCHAR(50),
          hide_birth_date INT DEFAULT 0,
          bio TEXT,
          heart_status VARCHAR(50) DEFAULT 'Available',
          avatar_base64 TEXT,
          can_post_news INT DEFAULT 0,
          password VARCHAR(255) NOT NULL,
          university VARCHAR(255)
        )""",
        """CREATE TABLE IF NOT EXISTS posts(
          id SERIAL PRIMARY KEY,
          user_id INT NOT NULL,
          content TEXT,
          media_base64 TEXT,
          media_type VARCHAR(50),
          "timestamp" VARCHAR(50) DEFAULT to_char(now() AT TIME ZONE 'UTC' + INTERVAL '5 hours', 'YYYY-MM-DD HH24:MI:SS')
        )""",
        """CREATE TABLE IF NOT EXISTS comments(
          id SERIAL PRIMARY KEY,
          post_id INT NOT NULL,
          user_id INT NOT NULL,
          parent_id INT,
          content TEXT NOT NULL,
          "timestamp" VARCHAR(50) DEFAULT to_char(now() AT TIME ZONE 'UTC' + INTERVAL '5 hours', 'YYYY-MM-DD HH24:MI:SS')
        )""",
        """CREATE TABLE IF NOT EXISTS likes(
          id SERIAL PRIMARY KEY,
          user_id INT NOT NULL,
          post_id INT NOT NULL,
          is_like INT NOT NULL,
          "timestamp" VARCHAR(50) DEFAULT to_char(now() AT TIME ZONE 'UTC' + INTERVAL '5 hours', 'YYYY-MM-DD HH24:MI:SS')
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS iup ON likes(user_id, post_id)""",
        """CREATE TABLE IF NOT EXISTS follows(
          follower_id INT NOT NULL,
          following_id INT NOT NULL,
          PRIMARY KEY(follower_id, following_id)
        )""",
        """CREATE TABLE IF NOT EXISTS school_news(
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL,
          author VARCHAR(255),
          "timestamp" VARCHAR(50) DEFAULT to_char(now() AT TIME ZONE 'UTC' + INTERVAL '5 hours', 'YYYY-MM-DD HH24:MI:SS')
        )""",
        """CREATE TABLE IF NOT EXISTS news_likes(
          id SERIAL PRIMARY KEY,
          user_id INT NOT NULL,
          news_id INT NOT NULL,
          "timestamp" VARCHAR(50) DEFAULT to_char(now() AT TIME ZONE 'UTC' + INTERVAL '5 hours', 'YYYY-MM-DD HH24:MI:SS')
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS iun ON news_likes(user_id, news_id)""",
        """CREATE TABLE IF NOT EXISTS news_comments(
          id SERIAL PRIMARY KEY,
          news_id INT NOT NULL,
          user_id INT NOT NULL,
          content TEXT NOT NULL,
          "timestamp" VARCHAR(50) DEFAULT to_char(now() AT TIME ZONE 'UTC' + INTERVAL '5 hours', 'YYYY-MM-DD HH24:MI:SS')
        )""",
        """CREATE TABLE IF NOT EXISTS comment_likes(
          id SERIAL PRIMARY KEY,
          user_id INT NOT NULL,
          comment_id INT NOT NULL,
          "timestamp" VARCHAR(50) DEFAULT to_char(now() AT TIME ZONE 'UTC' + INTERVAL '5 hours', 'YYYY-MM-DD HH24:MI:SS')
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS iucl ON comment_likes(user_id, comment_id)""",
        """CREATE TABLE IF NOT EXISTS news_comment_likes(
          id SERIAL PRIMARY KEY,
          user_id INT NOT NULL,
          comment_id INT NOT NULL,
          "timestamp" VARCHAR(50) DEFAULT to_char(now() AT TIME ZONE 'UTC' + INTERVAL '5 hours', 'YYYY-MM-DD HH24:MI:SS')
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS iuncl ON news_comment_likes(user_id, comment_id)""",
        """CREATE TABLE IF NOT EXISTS certificates(
          id SERIAL PRIMARY KEY,
          user_id INT NOT NULL,
          title TEXT NOT NULL,
          image_base64 TEXT,
          verified INT DEFAULT 1,
          "timestamp" VARCHAR(50) DEFAULT to_char(now() AT TIME ZONE 'UTC' + INTERVAL '5 hours', 'YYYY-MM-DD HH24:MI:SS')
        )""",
        """ALTER TABLE users ADD COLUMN IF NOT EXISTS university VARCHAR(255)""",
        """ALTER TABLE posts ADD COLUMN IF NOT EXISTS quoted_post_id INT""",
        """CREATE TABLE IF NOT EXISTS bookmarks(
          id SERIAL PRIMARY KEY,
          user_id INT NOT NULL,
          post_id INT NOT NULL,
          "timestamp" VARCHAR(50) DEFAULT to_char(now() AT TIME ZONE 'UTC' + INTERVAL '5 hours', 'YYYY-MM-DD HH24:MI:SS')
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS iubm ON bookmarks(user_id, post_id)""",
        """ALTER TABLE posts ADD COLUMN IF NOT EXISTS poll_ends_at VARCHAR(50)""",
        """CREATE TABLE IF NOT EXISTS poll_options(
          id SERIAL PRIMARY KEY,
          post_id INT NOT NULL,
          option_text TEXT NOT NULL,
          option_order INT DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS poll_votes(
          id SERIAL PRIMARY KEY,
          post_id INT NOT NULL,
          option_id INT NOT NULL,
          user_id INT NOT NULL,
          "timestamp" VARCHAR(50) DEFAULT to_char(now() AT TIME ZONE 'UTC' + INTERVAL '5 hours', 'YYYY-MM-DD HH24:MI:SS')
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS iupv ON poll_votes(post_id, user_id)""",
    ]
    # Each statement runs on its own so one failure (e.g. a stale/partial
    # previous deploy, or a table the migration already created slightly
    # differently) can never block the rest of the schema from being
    # created -- and any real error is printed to the Render logs instead
    # of silently aborting the whole batch.
    for stmt in statements:
        try:
            c.execute(stmt)
            c.commit()
        except Exception as e:
            print(f"[init] schema statement failed (continuing): {e}")
            try:
                c._conn.rollback()
            except Exception:
                pass
    c.commit(); c.close()


init()


# ---------------------------------------------------------------------------
# Cloudflare R2 (S3-compatible) object storage -- used for ALL images (post
# media, avatars, certificate photos). Text/post data stays in CockroachDB;
# only the raw image bytes live in R2, so the database never has to store
# or transfer big base64 blobs. The users/posts/certificates columns that
# used to hold raw base64 image data (avatar_base64, media_base64,
# image_base64) hold a plain R2 URL string instead -- column names were
# left as-is so the DB schema didn't need extra changes, only what's
# stored in them.
#
# Set these in your host's Environment settings, never commit real keys:
#   R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL,
#   R2_PUBLIC_URL, R2_BUCKET_NAME
# ---------------------------------------------------------------------------
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT = os.environ.get("R2_ENDPOINT_URL")
R2_PUBLIC_URL = (os.environ.get("R2_PUBLIC_URL") or "").rstrip("/")
R2_BUCKET = os.environ.get("R2_BUCKET_NAME", "chalker")

_r2 = None
if R2_ACCESS_KEY and R2_SECRET_KEY and R2_ENDPOINT:
    _r2 = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=_BotoConfig(signature_version="s3v4"),
        region_name="auto",
    )

# Images are compressed to JPEG client-side (max ~1280px) before upload, so
# 8MB is a generous ceiling -- this just guards direct API hits.
IMAGE_ALLOWED_EXT = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
IMAGE_MAX_SIZE = 8 * 1024 * 1024  # 8 MB
IMAGE_LIMIT_MSG = "Rasm hajmi 8MB dan oshmasligi va faqat (jpg, png, gif, webp) formatda bo'lishi kerak!"


def pub(r):
    d = dict(r); d.pop("password", None); return d
def err(m, s=400):
    return BigIntSafeJSONResponse({"error": m}, status_code=s)
def urow(c, uid):
    return c.execute("SELECT * FROM users WHERE id=%s", (uid,)).fetchone()
def news_rights(r):
    return bool(r and (r["username"] == "boss" or r["can_post_news"] == 1))
def clean_u(u):
    return u.strip().lower().lstrip("@")
# Username rule: at least 5 characters, letters/digits/underscore/dot only
# (no spaces or other symbols).
_USERNAME_RE = re.compile(r"^[a-z0-9_.]{5,}$")
_USERNAME_ERR = "Username kamida 5 belgidan iborat bo'lishi va faqat harf, raqam, \"_\" va \".\" belgilaridan tashkil topishi kerak!"
def valid_username(u):
    return bool(_USERNAME_RE.match(u or ""))

class PostCreate(BaseModel):
    user_id: int; content: str = ""; media_base64: Optional[str] = None; media_type: Optional[str] = None; quoted_post_id: Optional[int] = None
    poll_options: Optional[List[str]] = None; poll_minutes: Optional[int] = None
class PollVote(BaseModel):
    user_id: int; post_id: int; option_id: int
class PostEdit(BaseModel):
    user_id: int; post_id: int; content: str = ""
class PostDel(BaseModel):
    user_id: int; post_id: int
class LikeReq(BaseModel):
    user_id: int; post_id: int; is_like: int = 1
class CommentCreate(BaseModel):
    user_id: int; post_id: int; content: str; parent_id: Optional[int] = None
class FollowReq(BaseModel):
    follower_id: int; following_username: str
class NewsCreate(BaseModel):
    user_id: int; title: str
class NewsEdit(BaseModel):
    user_id: int; news_id: int; title: str
class NewsDel(BaseModel):
    user_id: int; news_id: int
class NewsLike(BaseModel):
    user_id: int; news_id: int
class NewsComment(BaseModel):
    user_id: int; news_id: int; content: str
class RightsReq(BaseModel):
    boss_id: int; target_username: str
class DeleteUserReq(BaseModel):
    boss_id: int; target_username: str
class RemoveFollowerReq(BaseModel):
    owner_id: int; follower_username: str
class CommentDel(BaseModel):
    user_id: int; comment_id: int
class NewsCommentDel(BaseModel):
    user_id: int; comment_id: int
class CommentLikeReq(BaseModel):
    user_id: int; comment_id: int
class NewsCommentLikeReq(BaseModel):
    user_id: int; comment_id: int
class CertCreate(BaseModel):
    boss_id: int; target_username: str; title: str; image_base64: Optional[str] = None
class CertDel(BaseModel):
    boss_id: int; cert_id: int
class CertSelfCreate(BaseModel):
    user_id: int; title: str; image_base64: Optional[str] = None
class CertSelfDel(BaseModel):
    user_id: int; cert_id: int
class BookmarkReq(BaseModel):
    user_id: int; post_id: int

# ---------------------------------------------------------------------------
# Static site icons (favicon / apple-touch-icon / manifest icons)
# ---------------------------------------------------------------------------
# Served as real files at well-known paths, base64-embedded here so nothing
# extra needs to be uploaded/deployed alongside main.py. Browser bookmark /
# speed-dial tiles generally fetch these paths directly and don't reliably
# pick up an inline SVG data: URI from the HTML <head>, which is why Chalker
# previously showed up with no icon next to other sites.
_FAVICON_16_PNG_B64 = "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAwklEQVR4nM2SMQqEMBBFf+wCWom1lWew8BqChWeJ3kGsvZdBC7HwCk71t1qXXQOu2WY/TPffIzNE4RXiXhQABJ7wwShP+EhwXfEUxHGMruswTRP2fce6rjDGOLv8HK01x3GkiLCqKkZRxKIo2DTNqesU1HVNkhyGwQW8jXOFNE0BANZavxssywIAyLLsUuBcQWtNay1FhGVZMgxD5nn+/Q0AMEkS9n3PeZ4pIty2jW3bnnp//JHuCNQPvHq+wEeiAOAB/kF5sMCXPF8AAAAASUVORK5CYII="
_FAVICON_32_PNG_B64 = "iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAABnUlEQVR4nO2Xu4rCQBiFT8I2BguNnSBBsLD0KZJiLKysRCzFVgTxXaztRVSw9SWElEa8gBEVU4j8Wwhh44WMbjLZBQ8MTIbJnG8O5M+MhMeiJ+O/leQ3EJbxU185AnOPl3w7IBpC9psVtiREs3tXkSfwPwEURUGj0cBgMMB8PofjONjv9zBNE+PxGM1mE+l0mns9eqXpuk7L5ZL8VK/Xudb7emXnjDH0+33I8jW4xWKBdruN0WiE0+mETCaDfD6PUqkEx3GCTSCZTJJt2+4ObdumbDb7UnpPGt/ETqfjibjVagVhzg8wnU49AJqmiQXYbDau+fF4DMqcuD/DRCLh9g+HA+9rvuIG2O12bj8ej4sHmM1mHgBN08QCDIdDz3O5XA4EAHizDmy3W7F1AAAVi0W6XC4uhGVZVKlUSFVVisVilMvliDFG3W6XqtVq8AAAyDAMWq1Wgf0L3jqQKIqCWq0GxhgKhQJSqRTO5zPW6zVM08RkMkGv14NlWb5rfU5EHwAZD65LAiX9iQSAaFKQfgKIhpDuOjcKqzbc+X0Dg8HRORPS3lYAAAAASUVORK5CYII="
_FAVICON_48_PNG_B64 = "iVBORw0KGgoAAAANSUhEUgAAADAAAAAwCAYAAABXAvmHAAACWElEQVR4nO2Zza7pUBiGv8pJu6kmQvxMtLPeADEQEQO3IBEhLsPAZYiZGBiInwGNGFcMRMykrkCCxEhCTDS+M9oStNirZ2t74k06+ZZ2PU+XpasLBY+DT9rfFeqnDVYBv80dr0PjQ1aFB9BguxWwMvx3rhgdeg0Wz4XVcVuwURBAew7YKhTY8+5fYvsR+AiYnY+A2fnzGxdNJpOQTqchkUiAIAjg8/mAZVnY7/ewXq9BURQYjUYwGAxgvV4b7g//1ZHJZHA+n+OrUVUVO50OulwuI/0aB2dZFhuNxsvgtwmFQuYJ0DSNsiwTw5suUKvVdMGm0ynm83nkeR4ZhkGO41AURSwWi9jv91FVVXMFIpEIns9nTfhyufz0fEEQsN1uYzAYNEdAkiRN+Gq1amhU3yLw9fWFx+PxDn6/36PH43mbAPGDLB6Pg9PpvKsPh0PY7Xakl/1xiAV4ntesz2YzYhiSEAv4/X7N+na7JYYhCbEARelu1bw1xAJ6dzoQCBDDkIRYYLlcatZjsRgxDGls/TNKLAAPHmSVSsUeAtFoVHcdVCqVnp4vCAK2Wi3zlhIAgPV6XVdiMplgLpfDcDiMNE2j2+1GURSxUChgr9fD0+mEiCavRhmGwfF4rCvxSkwVAADkOA6bzaZ9Bb6PbDaLiqK8DK6qKna7XUOvlL+ytZhKpa5e6r1eL7AsC4fDATabDSwWC5BlGSRJgtVqZaivz96o2fkImJ2PgNlxwIM/kW0Q6r8YAQB7jgIFcD0H7CRxYb39CtlB4opRaw5YWeKO7RmsVdZJupx/ARM6n1B6rCJ/AAAAAElFTkSuQmCC"
_FAVICON_ICO_B64 = "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"
_APPLE_TOUCH_ICON_B64 = "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"
_ICON_192_B64 = "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"
_ICON_512_B64 = "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qV+/vnr37q3LLrtM/fv3V48ePZSSEsyPP6EAwG8UACSlq666SlOmTLGOEWplZWX65z//qdmzZ+ujjz7SwoULA/2egUgkojPPPFO9evVS79691bt3b51//vmKxWLW0U4JBQB+owAgaY0fP14/+clPrGM4o7S0VF988YUWL16szz77TMuWLdPq1at9/4yB1NRUtWvXTmeffbY6d+6sbt26KTs7W127dlWjRo18zZJIFAD4jQKApBWNRvXRRx+pT58+1lGctmXLFq1Zs0YbN27Upk2btHnzZm3ZskV79uzR3r17tXfvXhUXF6usrEzl5eUqLy9XZWWlUlNTFYvFDp+0tDSlp6ercePGSk9PV5MmTZSZmanMzEy1bt1arVu3Vvv27dWhQ4ekeVVfExQA+I0CgKTWqlUrLVmyRG3atLGOAtQJBQB+C+a7YYBTtG3bNg0bNizQz6YBIIgoAEh6CxYs0NixY61jAEBSoQAgFCZOnKhHHnnEOgYAJA0KAELjZz/7mV5//XXrGACQFCgACI14PK4bb7xR8+bNs44CAIFHAUColJaWavDgwVqxYoV1FAAINAoAQmfPnj0aMGCACgsLraMAQGBRABBKGzduVN++fbVhwwbrKAAQSBQAhNa6devUt29fbdq0yToKAAQOBQChVlBQoL59+2rz5s3WUQAgUCgACL3//Oc/6tevHyUAAL6BAgAnrFmzRrm5uVq1apV1FAAIBAoAnLFhwwZdcsklWrRokXUUADBHAYBTdu3apX79+mnmzJnWUQDAFAUAzjlw4IAGDRqkyZMnW0cBADMUADipvLxcN954o375y1+qqqrKOg4A+I4CAKc99NBDGjRokPbt22cdBQB8RQGA86ZNm6acnBytXr3aOgoA+IYCAEhavXq1cnJy9O6771pHAQBfUACA/9q3b58GDx6ssWPH6tChQ9ZxAMBTFADgKI899phycnK0cuVK6ygA4BkKAFCNL774Qj179tQzzzxjHQUAPEEBAI7j4MGDuvXWWzVw4EB+oyCA0KEAACcxdepUdenSRU8++aTi8bh1HABICAoAcApKSkr0ox/9SN/5zne0Zs0a6zgAUGcUAKAG5s2bp+7du+uee+7RwYMHreMAQK1RAIAaOnTokO69916dc845evnll63jIMkVFBTo5ptv5tMo4buIJB5qAnWQm5ur8ePH68ILL7SOgiSyfv16/f73v9ekSZNUUVFhHQcOogAACRCJRDRixAj97ne/U+fOna3jIMDWrFmjhx9+WC+88ILKy8ut48BhFAAggVJTU3X99dfr7rvvVqdOnazjIEA+++wzPfTQQ3rrrbf4DZQIBAoA4IHU1FTdcMMN+s1vfqOOHTtax4GRqqoqTZs2TY888og++ugj6zjAESgAgIdSU1M1ZMgQ3XXXXerdu7d1HPikuLhYzz//vB599FGtXbvWOg5QLQoA4JOcnBzdddddGjp0qKLRqHUceCA/P19PP/20XnrpJZWUlFjHAU6IAgD4rF27dhozZoy+//3vKysryzoO6qi4uFivvPKKJk6cqPz8fOs4wCmjAABGUlNTNWDAAN18880aOHCg6tWrZx0Jp6iiokKzZs3S5MmT9e677+qrr76yjgTUGAUACIDmzZtr1KhRGjlypHr27GkdB9WoqqrSP//5T7322mt69dVXtWPHDutIQJ1QAICAad++vYYNG6ahQ4fqoosuUiQSsY7krMrKSn3yySd644039Oabb2rLli3WkYCEoQAAAdamTRsNHTpUgwYNUp8+fXTaaadZRwq9PXv2aObMmZo6daqmT5+uXbt2WUcCPEEBAJJE/fr1demllyovL095eXk677zzrCOFQkVFhRYtWqQPP/xQ77//vj799FNVVlZaxwI8RwEAklRWVpb69euniy++WL1791bXrl2VmppqHSvwysrKtGTJEs2fP19z5szR3LlztX//futYgO8oAEBINGzYUDk5OcrNzdVFF12k888/nx8zlLRu3Trl5+dr0aJF+vTTT7V48WKVlpZaxwLMUQCAEGvWrJmys7PVrVs3ZWdnKzs7W+eee64aNmxoHS3hSkpKtGrVKq1YsULLli3T559/rvz8fO3Zs8c6GhBIFADAQa1atdJZZ52ljh07HvFnu3bt1KpVq8C+2XD37t0qLCzUunXrVFBQoIKCAq1du1arV6/Whg0bFI/z7Qw4VRQAAMfIyMhQZmbm4dOqVSs1a9ZMjRs3rvakpaUpFospGo0e/vN/f52amqqqqipVVFSooqJClZWVKisr08GDB3XgwIHDp6SkRHv27NGuXbu0e/du7dq1S9u3b9eWLVu0efNmFRUVsboHEogCAACAg1KsAwAAAP9RAAAAcBAFAAAAB1EAAABwEAUAAAAHUQAAAHAQBQAAAAdRAAAAcBAFAAAAB1EAAABwEAUAAAAHUQAAAHAQBQAAAAdRAAAAcBAFAAAAB1EAAABwEAUAAAAHUQAAAHAQBQAAAAdRAAAAcBAFAAAAB1EAAABwEAUAAAAHUQAAAHAQBQAAAAdRAAAAcBAFAAAAB1EAAABwEAUAAAAHpUiKWIcAAAC+irABAADAQRQAAAAcRAEAAMBBFAAAABxEAQAAwEEUAAAAHEQBAADAQf8rAHwWAAAAbohIbAAAAHASBQAAAAdRAAAAcBAFAAAAB32zAPBGQAAAwu3wrGcDAACAgygAAAA4iAIAAICDji4AvA8AAIBwOmLGswEAAMBBFAAAABxEAQAAwEHVFQDeBwAAQLgcM9vZAAAA4CAKAAAADjpeAeAxAAAA4VDtTGcDAACAgygAAAA46EQFgMcAAAAkt+POcjYAAAA4iAIAAICDTlYAeAwAAEByOuEMZwMAAICDTqUAsAUAACC5nHR2swEAAMBBFAAAABx0qgWAxwAAACSHU5rZbAAAAHBQTQoAWwAAAILtlGc1GwAAABxU0wLAFgAAgGCq0YxmAwAAgINqUwDYAgAAECw1ns1sAAAAcFBtCwBbAAAAgqFWM5kNAAAADqpLAWALAACArVrP4rpuACgBAADYqNMM5hEAAAAOSkQBYAsAAIC/6jx72QAAAOCgRBUAtgAAAPgjITM3kRsASgAAAN5K2KzlEQAAAA5KdAFgCwAAgDcSOmO92ABQAgAASKyEz1avHgFQAgAASAxPZirvAQAAwEFeFgC2AAAA1I1ns9TrDQAlAACA2vF0hvrxCIASAABAzXg+O3kPAAAADvKrALAFAADg1PgyM/3cAFACAAA4Md9mpd+PACgBAABUz9cZafEeAEoAAABH8n02Wr0JkBIAAMDXTGai5U8BUAIAAK4zm4XWPwZICQAAuMp0BloXAIkSAABwj/nsC0IBkAJwIQAA8EkgZl5QCoAUkAsCAICHAjPrglQApABdGAAAEixQMy5oBUAK2AUCACABAjfbglgApABeKAAAaimQMy2oBUAK6AUDAKAGAjvLglwApABfOAAATiLQMyzoBUAK+AUEAKAagZ9dgQ94lLh1AAAATiBp5moybAC+KWkuLADAOUk1o5KtAEhJdoEBAE5IutmUdIGPwiMBAIClpJ2jybgB+KakvfAAgKSX1DMo2QuAlOQ3AACQlJJ+9iT9F3AUHgkAALwUmrkZhg3AN4XmxgAAAidUMyZUX8xR2AYAABIhlLMylF/UUSgCAIDaCPWMDNsjgOqE+gYCADwR+tkR+i/wKGwDAAAn4sxcdOYLPQpFAADwTc7NQ+e+4KNQBADAbc7OQWe/8KNQBADALc7PP+cvwFEoAgAQbsy9/+JCHB9lAADCgVlXDS7KyVEEACA5MeNOgItTM5QBAAg25top4kLVHmUAAIKBWVYLXLTEoAwAgL+YX3XEBfQGhQAAEot5lWBcUH9QCACgZphPHuMC26EUAMDXmEUGuOjBRUEAEBbMmgD6f2YgT1SRWbxLAAAAAElFTkSuQmCC"

@app.get("/ping")
def ping():
    return "OK"

@app.get("/ping-db")
def ping_db():
    """Hit this from an external cron service (e.g. cron-job.org) every
    30-45 minutes to keep the pool warm / catch a broken connection early."""
    try:
        c = db()
        try:
            c.execute("SELECT 1").fetchall()
        finally:
            c.close()
        return "OK"
    except Exception as e:
        return BigIntSafeJSONResponse({"error": str(e)}, status_code=503)

@app.get("/", response_class=HTMLResponse)
def index():
    with open(INDEX, "r", encoding="utf-8") as f:
        html = f.read()
    return HTMLResponse(html, headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

_ICON_CACHE_HEADERS = {"Cache-Control": "public, max-age=86400"}

@app.get("/favicon.ico")
def favicon_ico():
    return Response(base64.b64decode(_FAVICON_ICO_B64), media_type="image/x-icon", headers=_ICON_CACHE_HEADERS)

@app.get("/favicon-16.png")
def favicon_16():
    return Response(base64.b64decode(_FAVICON_16_PNG_B64), media_type="image/png", headers=_ICON_CACHE_HEADERS)

@app.get("/favicon-32.png")
def favicon_32():
    return Response(base64.b64decode(_FAVICON_32_PNG_B64), media_type="image/png", headers=_ICON_CACHE_HEADERS)

@app.get("/favicon-48.png")
def favicon_48():
    return Response(base64.b64decode(_FAVICON_48_PNG_B64), media_type="image/png", headers=_ICON_CACHE_HEADERS)

@app.get("/apple-touch-icon.png")
def apple_touch_icon():
    return Response(base64.b64decode(_APPLE_TOUCH_ICON_B64), media_type="image/png", headers=_ICON_CACHE_HEADERS)

@app.get("/icon-192.png")
def icon_192():
    return Response(base64.b64decode(_ICON_192_B64), media_type="image/png", headers=_ICON_CACHE_HEADERS)

@app.get("/icon-512.png")
def icon_512():
    return Response(base64.b64decode(_ICON_512_B64), media_type="image/png", headers=_ICON_CACHE_HEADERS)

@app.get("/site.webmanifest")
def site_webmanifest():
    manifest = {
        "name": "Chalker",
        "short_name": "Chalker",
        "icons": [
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png"},
        ],
        "theme_color": "#000000",
        "background_color": "#000000",
        "display": "standalone",
    }
    return JSONResponse(manifest, headers=_ICON_CACHE_HEADERS)

@app.get("/api/check_username")
def check_username(username: str, exclude_id: Optional[int] = None):
    u = clean_u(username)
    if not u:
        return {"available": False}
    c = db()
    q = "SELECT id FROM users WHERE username=%s" + (" AND id!=%s" if exclude_id else "")
    row = c.execute(q, (u, exclude_id) if exclude_id else (u,)).fetchone()
    c.close()
    return {"available": row is None}

@app.post("/api/register")
def register(username: str = Form(...), fullname: str = Form(...), password: str = Form(...)):
    u = clean_u(username)
    if not u or not password: return err("Username va parol majburiy!")
    c = db()
    if c.execute("SELECT id FROM users WHERE username=%s", (u,)).fetchone():
        c.close(); return err("Bu username band!")
    if not valid_username(u):
        c.close(); return err(_USERNAME_ERR)
    if len(password) < 4:
        c.close(); return err("Parol kamida 4 belgi!")
    cur = c.execute("INSERT INTO users(username,fullname,password) VALUES(%s,%s,%s) RETURNING id", (u, fullname.strip(), password))
    new_id = cur.fetchone()["id"]
    c.commit(); r = urow(c, new_id); c.close()
    return {"user": pub(r)}

@app.post("/api/login")
def login(username: str = Form(...), password: str = Form(...)):
    c = db()
    r = c.execute("SELECT * FROM users WHERE username=%s AND password=%s", (clean_u(username), password)).fetchone()
    c.close()
    return {"user": pub(r)} if r else err("Username yoki parol xato!", 401)

@app.post("/api/account/update")
def account(user_id: int = Form(...), current_password: str = Form(...),
            new_username: str = Form(""), new_password: str = Form("")):
    c = db()
    r = c.execute("SELECT * FROM users WHERE id=%s AND password=%s", (user_id, current_password)).fetchone()
    if not r: c.close(); return err("Joriy parol xato!", 401)
    nu = clean_u(new_username)
    if nu and nu != r["username"]:
        if c.execute("SELECT id FROM users WHERE username=%s", (nu,)).fetchone():
            c.close(); return err("Bu username band!")
        if not valid_username(nu):
            c.close(); return err(_USERNAME_ERR)
        c.execute("UPDATE users SET username=%s WHERE id=%s", (nu, user_id))
    if new_password:
        if len(new_password) < 4: c.close(); return err("Yangi parol kamida 4 belgi!")
        c.execute("UPDATE users SET password=%s WHERE id=%s", (new_password, user_id))
    c.commit(); r = urow(c, user_id); c.close()
    return {"user": pub(r)}

@app.post("/api/profile/update")
def profile(user_id: int = Form(...), fullname: str = Form(""), school_class: str = Form(""),
            school_name: str = Form(""), country: str = Form(""), region: str = Form(""),
            district: str = Form(""), role: str = Form("student"), bio: str = Form(""),
            birth_date: str = Form(""), hide_birth_date: int = Form(0),
            heart_status: str = Form("Available"), avatar_base64: str = Form(""),
            university: str = Form("")):
    c = db()
    c.execute("""UPDATE users SET fullname=%s,school_class=%s,school_name=%s,country=%s,region=%s,district=%s,
              role=%s,bio=%s,birth_date=%s,hide_birth_date=%s,heart_status=%s,university=%s WHERE id=%s""",
              (fullname.strip(), school_class, school_name, country, region, district, role, bio.strip(),
               birth_date, int(hide_birth_date), heart_status, university.strip(), user_id))
    if avatar_base64:
        c.execute("UPDATE users SET avatar_base64=%s WHERE id=%s", (avatar_base64, user_id))
    c.commit(); r = urow(c, user_id); c.close()
    return {"user": pub(r)} if r else err("Topilmadi!", 404)

@app.get("/api/users/{username}")
def get_user(username: str, viewer_id: Optional[int] = None):
    c = db()
    r = c.execute("SELECT * FROM users WHERE username=%s", (clean_u(username),)).fetchone()
    if not r: c.close(); return err("Foydalanuvchi topilmadi!", 404)
    d = pub(r)
    d["followers"] = c.execute("SELECT COUNT(*) AS cnt FROM follows WHERE following_id=%s", (r["id"],)).fetchone()["cnt"]
    d["following"] = c.execute("SELECT COUNT(*) AS cnt FROM follows WHERE follower_id=%s", (r["id"],)).fetchone()["cnt"]
    d["is_following"] = bool(viewer_id and c.execute(
        "SELECT 1 FROM follows WHERE follower_id=%s AND following_id=%s", (viewer_id, r["id"])).fetchone())
    if int(d.get("hide_birth_date") or 0) == 1 and (viewer_id is None or int(viewer_id) != r["id"]):
        d["birth_date"] = None
    c.close(); return d

@app.get("/api/users/{username}/followers")
def followers_list(username: str, viewer_id: Optional[int] = None):
    c = db()
    u = c.execute("SELECT id FROM users WHERE username=%s", (clean_u(username),)).fetchone()
    if not u: c.close(); return err("Topilmadi!", 404)
    v = viewer_id if viewer_id is not None else -1
    rows = c.execute("""SELECT us.id,us.username,us.fullname,us.avatar_base64,us.can_post_news,us.school_name,
        (SELECT 1 FROM follows WHERE follower_id=%s AND following_id=us.id) is_following
        FROM follows f JOIN users us ON us.id=f.follower_id
        WHERE f.following_id=%s ORDER BY us.username""", (v, u["id"])).fetchall()
    c.close(); return [{**dict(r), "is_following": bool(r["is_following"])} for r in rows]

@app.get("/api/users/{username}/following")
def following_list(username: str, viewer_id: Optional[int] = None):
    c = db()
    u = c.execute("SELECT id FROM users WHERE username=%s", (clean_u(username),)).fetchone()
    if not u: c.close(); return err("Topilmadi!", 404)
    v = viewer_id if viewer_id is not None else -1
    rows = c.execute("""SELECT us.id,us.username,us.fullname,us.avatar_base64,us.can_post_news,us.school_name,
        (SELECT 1 FROM follows WHERE follower_id=%s AND following_id=us.id) is_following
        FROM follows f JOIN users us ON us.id=f.following_id
        WHERE f.follower_id=%s ORDER BY us.username""", (v, u["id"])).fetchall()
    c.close(); return [{**dict(r), "is_following": bool(r["is_following"])} for r in rows]

@app.get("/api/certificates")
def certificates(username: str):
    c = db()
    u = c.execute("SELECT id FROM users WHERE username=%s", (clean_u(username),)).fetchone()
    if not u: c.close(); return []
    rows = c.execute("SELECT * FROM certificates WHERE user_id=%s ORDER BY id DESC", (u["id"],)).fetchall()
    c.close(); return [dict(r) for r in rows]

@app.post("/api/certificates/create")
def certificate_create(b: CertCreate):
    c = db(); boss = urow(c, b.boss_id)
    if not boss or boss["username"] != "boss": c.close(); return err("Faqat @boss!", 403)
    if not b.title.strip(): c.close(); return err("Nomi majburiy!")
    tg = c.execute("SELECT id FROM users WHERE username=%s", (clean_u(b.target_username),)).fetchone()
    if not tg: c.close(); return err("Topilmadi!", 404)
    c.execute("INSERT INTO certificates(user_id,title,image_base64) VALUES(%s,%s,%s)",
              (tg["id"], b.title.strip(), b.image_base64))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/certificates/delete")
def certificate_delete(b: CertDel):
    c = db(); boss = urow(c, b.boss_id)
    if not boss or boss["username"] != "boss": c.close(); return err("Faqat @boss!", 403)
    c.execute("DELETE FROM certificates WHERE id=%s", (b.cert_id,))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/certificates/self_create")
def certificate_self_create(b: CertSelfCreate):
    if not b.title.strip(): return err("Nomi majburiy!")
    c = db()
    if not urow(c, b.user_id): c.close(); return err("Foydalanuvchi topilmadi!", 404)
    c.execute("INSERT INTO certificates(user_id,title,image_base64) VALUES(%s,%s,%s)",
              (b.user_id, b.title.strip(), b.image_base64))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/certificates/self_delete")
def certificate_self_delete(b: CertSelfDel):
    c = db()
    r = c.execute("SELECT user_id FROM certificates WHERE id=%s", (b.cert_id,)).fetchone()
    if not r: c.close(); return err("Topilmadi!", 404)
    requester = urow(c, b.user_id)
    if r["user_id"] != b.user_id and not (requester and requester["username"] == "boss"):
        c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM certificates WHERE id=%s", (b.cert_id,))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/upload/image")
async def upload_image(file: UploadFile = File(...)):
    """Generic image upload used for post media, avatars, and certificate
    photos. Uploads the raw bytes straight to R2 and returns the public
    URL -- the caller then stores that URL in whichever *_base64 column it
    used to store the raw base64 data in (see the R2 comment block above)."""
    try:
        if _r2 is None:
            return err("Fayl xizmati sozlanmagan: R2 kalitlari (.env) topilmadi!", 500)
        orig_name = file.filename or "rasm.jpg"
        ext = os.path.splitext(orig_name)[1].lower()
        if ext not in IMAGE_ALLOWED_EXT:
            # Browser-generated blobs (canvas.toBlob) often arrive without a
            # clean filename/extension -- default to jpg since that's what
            # the frontend's image compressor always outputs.
            ext = ".jpg"
        data = await file.read()
        if len(data) > IMAGE_MAX_SIZE:
            return err(IMAGE_LIMIT_MSG, 400)
        key = f"images/{uuid.uuid4().hex}{ext}"
        _r2.put_object(
            Bucket=R2_BUCKET,
            Key=key,
            Body=data,
            ContentType=file.content_type or "image/jpeg",
        )
        return {"url": f"{R2_PUBLIC_URL}/{key}"}
    except Exception as e:
        return err(f"Kutilmagan xatolik: {e}", 500)

@app.post("/api/posts/create")
def post_create(b: PostCreate):
    opts = [o.strip() for o in (b.poll_options or []) if o and o.strip()][:4]
    has_poll = len(opts) >= 2
    if not b.content.strip() and not b.media_base64 and not b.quoted_post_id and not has_poll:
        return err("Post bo'sh!")
    c = db()
    mins = b.poll_minutes if (has_poll and b.poll_minutes and b.poll_minutes > 0) else None
    if has_poll and not mins:
        mins = 24 * 60  # default: 1 kunlik so'rovnoma
    row = c.execute(
        """INSERT INTO posts(user_id,content,media_base64,media_type,quoted_post_id,poll_ends_at)
           VALUES(%s,%s,%s,%s,%s,
             CASE WHEN %s THEN to_char(now() AT TIME ZONE 'UTC' + INTERVAL '5 hours' + (%s::text || ' minutes')::interval,'YYYY-MM-DD HH24:MI:SS') ELSE NULL END)
           RETURNING id""",
        (b.user_id, b.content.strip(), b.media_base64, b.media_type, b.quoted_post_id, has_poll, mins),
    ).fetchone()
    pid = row["id"]
    if has_poll:
        for i, opt in enumerate(opts):
            c.execute("INSERT INTO poll_options(post_id,option_text,option_order) VALUES(%s,%s,%s)", (pid, opt[:100], i))
    c.commit(); c.close(); return {"success": True, "post_id": pid}

@app.post("/api/polls/vote")
def poll_vote(b: PollVote):
    c = db()
    post = c.execute(
        """SELECT poll_ends_at, (poll_ends_at IS NOT NULL AND poll_ends_at::timestamp < (now() AT TIME ZONE 'UTC' + INTERVAL '5 hours')) AS expired
           FROM posts WHERE id=%s""", (b.post_id,)).fetchone()
    if not post or not post["poll_ends_at"]:
        c.close(); return err("So'rovnoma topilmadi!", 404)
    if post["expired"]:
        c.close(); return err("So'rovnoma muddati tugagan!", 400)
    opt = c.execute("SELECT id FROM poll_options WHERE id=%s AND post_id=%s", (b.option_id, b.post_id)).fetchone()
    if not opt:
        c.close(); return err("Noto'g'ri tanlov!", 404)
    c.execute(
        """INSERT INTO poll_votes(post_id,option_id,user_id) VALUES(%s,%s,%s)
           ON CONFLICT (post_id,user_id) DO UPDATE SET option_id=EXCLUDED.option_id""",
        (b.post_id, b.option_id, b.user_id))
    c.commit()
    opts = c.execute(
        """SELECT po.id,(SELECT COUNT(*) FROM poll_votes pv WHERE pv.option_id=po.id) votes_count
           FROM poll_options po WHERE po.post_id=%s ORDER BY po.option_order""", (b.post_id,)).fetchall()
    c.close()
    return {"success": True, "options": [dict(o) for o in opts], "my_option_id": b.option_id}

@app.post("/api/posts/update")
def post_update(b: PostEdit):
    c = db()
    r = c.execute("SELECT user_id FROM posts WHERE id=%s", (b.post_id,)).fetchone()
    if not r or r["user_id"] != b.user_id: c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("UPDATE posts SET content=%s WHERE id=%s", (b.content.strip(), b.post_id))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/posts/delete")
def post_delete(b: PostDel):
    c = db()
    r = c.execute("SELECT user_id FROM posts WHERE id=%s", (b.post_id,)).fetchone()
    if not r or r["user_id"] != b.user_id: c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM posts WHERE id=%s", (b.post_id,))
    c.execute("DELETE FROM likes WHERE post_id=%s", (b.post_id,))
    c.execute("DELETE FROM comments WHERE post_id=%s", (b.post_id,))
    c.execute("DELETE FROM bookmarks WHERE post_id=%s", (b.post_id,))
    c.execute("DELETE FROM poll_votes WHERE post_id=%s", (b.post_id,))
    c.execute("DELETE FROM poll_options WHERE post_id=%s", (b.post_id,))
    c.commit(); c.close(); return {"success": True}

# Shared SELECT used by /api/posts, /api/posts/saved and /api/posts/liked --
# keeps like/comment/bookmark counts and the (optional) quoted-post preview
# consistent across all three listings instead of duplicating the join.
_POSTS_SELECT = """SELECT p.id,p.user_id,p.content,p.media_base64,p.media_type,p."timestamp",p.quoted_post_id,p.poll_ends_at,
        u.username,u.fullname,u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM likes l WHERE l.post_id=p.id AND l.is_like=1) likes_count,
        (SELECT COUNT(*) FROM comments cm WHERE cm.post_id=p.id) comments_count,
        (SELECT COUNT(*) FROM bookmarks bmc WHERE bmc.post_id=p.id) bookmarks_count,
        (SELECT COUNT(*) FROM posts qc WHERE qc.quoted_post_id=p.id) reposts_count,
        (SELECT l.is_like FROM likes l WHERE l.post_id=p.id AND l.user_id=%s) my_status,
        (SELECT 1 FROM bookmarks bm WHERE bm.post_id=p.id AND bm.user_id=%s) my_bookmark,
        qp.content quoted_content, qp.media_base64 quoted_media_base64, qp.media_type quoted_media_type,
        qp."timestamp" quoted_timestamp, qu.username quoted_username, qu.fullname quoted_fullname,
        qu.avatar_base64 quoted_avatar_base64
        FROM posts p JOIN users u ON u.id=p.user_id
        LEFT JOIN posts qp ON qp.id=p.quoted_post_id
        LEFT JOIN users qu ON qu.id=qp.user_id"""

def _attach_polls(c, rows, viewer_id):
    """Mutates each row dict in place, adding a "poll" key (or None) with
    its options, vote counts and the viewer's own vote -- kept as a
    separate follow-up query since building an options-as-array directly
    in the main JOIN would be awkward in plain psycopg2."""
    ids = [r["id"] for r in rows if r.get("poll_ends_at")]
    if not ids:
        for r in rows:
            r["poll"] = None
        return rows
    opts = c.execute(
        """SELECT po.id,po.post_id,po.option_text,po.option_order,
           (SELECT COUNT(*) FROM poll_votes pv WHERE pv.option_id=po.id) votes_count
           FROM poll_options po WHERE po.post_id = ANY(%s) ORDER BY po.post_id, po.option_order""",
        (ids,)).fetchall()
    my_votes = c.execute(
        "SELECT post_id, option_id FROM poll_votes WHERE user_id=%s AND post_id = ANY(%s)",
        (viewer_id, ids)).fetchall()
    my_map = {r["post_id"]: r["option_id"] for r in my_votes}
    by_post = {}
    for o in opts:
        by_post.setdefault(o["post_id"], []).append(dict(o))
    for r in rows:
        if r.get("poll_ends_at"):
            options = by_post.get(r["id"], [])
            total = sum(o["votes_count"] for o in options)
            r["poll"] = {"ends_at": r["poll_ends_at"], "options": options, "total_votes": total, "my_vote": my_map.get(r["id"])}
        else:
            r["poll"] = None
    return rows

@app.get("/api/posts")
def posts(user_id: Optional[int] = None, author: Optional[str] = None):
    v = user_id if user_id is not None else -1
    c = db()
    sql = _POSTS_SELECT
    params = [v, v]
    if author:
        sql += " WHERE u.username=%s"
        params.append(clean_u(author))
    sql += " ORDER BY p.id DESC"
    rows = [dict(r) for r in c.execute(sql, tuple(params)).fetchall()]
    _attach_polls(c, rows, v)
    c.close()
    # no-store: har doim like/comment holati bo'yicha eng so'nggi
    # ma'lumot qaytsin -- brauzer/Telegram WebView bu GET javobini
    # o'zi keshlab, keyingi safar layk bosilgan-bosilmaganini eski
    # holatda ko'rsatib qo'ymasin.
    return BigIntSafeJSONResponse(rows, headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/api/posts/saved")
def posts_saved(user_id: int):
    """Posts the user has bookmarked, most recently saved first."""
    c = db()
    sql = _POSTS_SELECT + " JOIN bookmarks bm2 ON bm2.post_id=p.id AND bm2.user_id=%s ORDER BY bm2.id DESC"
    rows = [dict(r) for r in c.execute(sql, (user_id, user_id, user_id)).fetchall()]
    _attach_polls(c, rows, user_id)
    c.close()
    return BigIntSafeJSONResponse(rows, headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/api/posts/liked")
def posts_liked(user_id: int):
    """Posts the user has liked, most recently liked first."""
    c = db()
    sql = _POSTS_SELECT + " JOIN likes lk2 ON lk2.post_id=p.id AND lk2.user_id=%s AND lk2.is_like=1 ORDER BY lk2.id DESC"
    rows = [dict(r) for r in c.execute(sql, (user_id, user_id, user_id)).fetchall()]
    _attach_polls(c, rows, user_id)
    c.close()
    return BigIntSafeJSONResponse(rows, headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.post("/api/posts/bookmark")
def post_bookmark(b: BookmarkReq):
    c = db()
    if c.execute("SELECT 1 FROM bookmarks WHERE user_id=%s AND post_id=%s", (b.user_id, b.post_id)).fetchone():
        c.execute("DELETE FROM bookmarks WHERE user_id=%s AND post_id=%s", (b.user_id, b.post_id)); bk = False
    else:
        c.execute("INSERT INTO bookmarks(user_id,post_id) VALUES(%s,%s)", (b.user_id, b.post_id)); bk = True
    c.commit(); c.close(); return {"bookmarked": bk}

@app.post("/api/posts/like")
def post_like(b: LikeReq):
    c = db()
    r = c.execute("SELECT is_like FROM likes WHERE user_id=%s AND post_id=%s", (b.user_id, b.post_id)).fetchone()
    if r and r["is_like"] == b.is_like:
        c.execute("DELETE FROM likes WHERE user_id=%s AND post_id=%s", (b.user_id, b.post_id)); liked = False
    elif r:
        c.execute("UPDATE likes SET is_like=%s WHERE user_id=%s AND post_id=%s", (b.is_like, b.user_id, b.post_id)); liked = True
    else:
        c.execute("INSERT INTO likes(user_id,post_id,is_like) VALUES(%s,%s,%s)", (b.user_id, b.post_id, b.is_like)); liked = True
    c.commit(); c.close(); return {"liked": liked}

@app.post("/api/comments/create")
def comment_create(b: CommentCreate):
    if not b.content.strip(): return err("Komment bo'sh!")
    c = db()
    c.execute("INSERT INTO comments(post_id,user_id,parent_id,content) VALUES(%s,%s,%s,%s)",
              (b.post_id, b.user_id, b.parent_id, b.content.strip()))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/comments")
def comments(post_id: int, viewer_id: Optional[int] = None):
    v = viewer_id if viewer_id is not None else -1
    c = db()
    rows = c.execute("""SELECT c.id,c.parent_id,c.content,c."timestamp",u.username,u.fullname,
        u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM comment_likes cl WHERE cl.comment_id=c.id) likes_count,
        (SELECT 1 FROM comment_likes cl WHERE cl.comment_id=c.id AND cl.user_id=%s) my_like
        FROM comments c JOIN users u ON u.id=c.user_id
        WHERE c.post_id=%s ORDER BY c.id ASC""", (v, post_id)).fetchall()
    c.close()
    return BigIntSafeJSONResponse([dict(r) for r in rows], headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.post("/api/comments/like")
def comment_like(b: CommentLikeReq):
    c = db()
    if c.execute("SELECT 1 FROM comment_likes WHERE user_id=%s AND comment_id=%s", (b.user_id, b.comment_id)).fetchone():
        c.execute("DELETE FROM comment_likes WHERE user_id=%s AND comment_id=%s", (b.user_id, b.comment_id)); liked = False
    else:
        c.execute("INSERT INTO comment_likes(user_id,comment_id) VALUES(%s,%s)", (b.user_id, b.comment_id)); liked = True
    cnt = c.execute("SELECT COUNT(*) cnt FROM comment_likes WHERE comment_id=%s", (b.comment_id,)).fetchone()["cnt"]
    c.commit(); c.close(); return {"liked": liked, "count": cnt}

@app.post("/api/comments/delete")
def comment_delete(b: CommentDel):
    c = db()
    r = c.execute("SELECT user_id FROM comments WHERE id=%s", (b.comment_id,)).fetchone()
    if not r: c.close(); return err("Komment topilmadi!", 404)
    requester = urow(c, b.user_id)
    if r["user_id"] != b.user_id and not (requester and requester["username"] == "boss"):
        c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM comments WHERE id=%s OR parent_id=%s", (b.comment_id, b.comment_id))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/search")
def search(q: str = "", viewer_id: Optional[int] = None):
    q = q.strip()
    if not q:
        return []
    c = db()
    like = f"%{q}%"
    v = viewer_id if viewer_id is not None else -1
    rows = c.execute("""SELECT id,username,fullname,avatar_base64,can_post_news,school_name,
        (SELECT 1 FROM follows WHERE follower_id=%s AND following_id=users.id) is_following
        FROM users WHERE username ILIKE %s OR fullname ILIKE %s
        ORDER BY CASE WHEN username ILIKE %s THEN 0 ELSE 1 END, username ASC
        LIMIT 25""",
        (v, like, like, q + "%")).fetchall()
    c.close()
    return [{**dict(r), "is_following": bool(r["is_following"])} for r in rows]

@app.post("/api/users/follow")
def follow(b: FollowReq):
    c = db()
    tg = c.execute("SELECT id FROM users WHERE username=%s", (clean_u(b.following_username),)).fetchone()
    if not tg: c.close(); return err("Topilmadi!", 404)
    if tg["id"] == b.follower_id: c.close(); return err("O'zingizga follow bosolmaysiz!")
    if c.execute("SELECT 1 FROM follows WHERE follower_id=%s AND following_id=%s", (b.follower_id, tg["id"])).fetchone():
        c.execute("DELETE FROM follows WHERE follower_id=%s AND following_id=%s", (b.follower_id, tg["id"])); f = False
    else:
        c.execute("INSERT INTO follows VALUES(%s,%s)", (b.follower_id, tg["id"])); f = True
    c.commit(); c.close(); return {"following": f}

@app.post("/api/news/create")
def news_create(b: NewsCreate):
    c = db(); r = urow(c, b.user_id)
    if not news_rights(r): c.close(); return err("Huquq yo'q!", 403)
    c.execute("INSERT INTO school_news(title,author) VALUES(%s,%s)", (b.title.strip(), r["username"]))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/news/update")
def news_update(b: NewsEdit):
    c = db()
    if not news_rights(urow(c, b.user_id)): c.close(); return err("Huquq yo'q!", 403)
    c.execute("UPDATE school_news SET title=%s WHERE id=%s", (b.title.strip(), b.news_id))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/news/delete")
def news_delete(b: NewsDel):
    c = db()
    if not news_rights(urow(c, b.user_id)): c.close(); return err("Huquq yo'q!", 403)
    c.execute("DELETE FROM school_news WHERE id=%s", (b.news_id,))
    c.execute("DELETE FROM news_likes WHERE news_id=%s", (b.news_id,))
    c.execute("DELETE FROM news_comments WHERE news_id=%s", (b.news_id,))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/news")
def news(user_id: Optional[int] = None):
    v = user_id if user_id is not None else -1
    c = db()
    rows = c.execute("""SELECT n.*,
        (SELECT COUNT(*) FROM news_likes l WHERE l.news_id=n.id) likes_count,
        (SELECT COUNT(*) FROM news_comments m WHERE m.news_id=n.id) comments_count,
        (SELECT 1 FROM news_likes l WHERE l.news_id=n.id AND l.user_id=%s) my_like
        FROM school_news n ORDER BY n.id DESC LIMIT 20""", (v,)).fetchall()
    c.close(); return [dict(r) for r in rows]

@app.post("/api/news/like")
def news_like(b: NewsLike):
    c = db()
    if c.execute("SELECT 1 FROM news_likes WHERE user_id=%s AND news_id=%s", (b.user_id, b.news_id)).fetchone():
        c.execute("DELETE FROM news_likes WHERE user_id=%s AND news_id=%s", (b.user_id, b.news_id)); lk = False
    else:
        c.execute("INSERT INTO news_likes(user_id,news_id) VALUES(%s,%s)", (b.user_id, b.news_id)); lk = True
    c.commit(); c.close(); return {"liked": lk}

@app.post("/api/news/comments/create")
def news_comment(b: NewsComment):
    if not b.content.strip(): return err("Komment bo'sh!")
    c = db()
    c.execute("INSERT INTO news_comments(news_id,user_id,content) VALUES(%s,%s,%s)",
              (b.news_id, b.user_id, b.content.strip()))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/news/comments")
def news_comments(news_id: int, viewer_id: Optional[int] = None):
    v = viewer_id if viewer_id is not None else -1
    c = db()
    rows = c.execute("""SELECT m.id,m.content,m."timestamp",u.username,u.fullname,u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM news_comment_likes cl WHERE cl.comment_id=m.id) likes_count,
        (SELECT 1 FROM news_comment_likes cl WHERE cl.comment_id=m.id AND cl.user_id=%s) my_like
        FROM news_comments m JOIN users u ON u.id=m.user_id WHERE m.news_id=%s ORDER BY m.id ASC""",
        (v, news_id)).fetchall()
    c.close(); return [dict(r) for r in rows]

@app.post("/api/news/comments/like")
def news_comment_like(b: NewsCommentLikeReq):
    c = db()
    if c.execute("SELECT 1 FROM news_comment_likes WHERE user_id=%s AND comment_id=%s", (b.user_id, b.comment_id)).fetchone():
        c.execute("DELETE FROM news_comment_likes WHERE user_id=%s AND comment_id=%s", (b.user_id, b.comment_id)); liked = False
    else:
        c.execute("INSERT INTO news_comment_likes(user_id,comment_id) VALUES(%s,%s)", (b.user_id, b.comment_id)); liked = True
    cnt = c.execute("SELECT COUNT(*) cnt FROM news_comment_likes WHERE comment_id=%s", (b.comment_id,)).fetchone()["cnt"]
    c.commit(); c.close(); return {"liked": liked, "count": cnt}

@app.post("/api/news/comments/delete")
def news_comment_delete(b: NewsCommentDel):
    c = db()
    r = c.execute("SELECT user_id FROM news_comments WHERE id=%s", (b.comment_id,)).fetchone()
    if not r: c.close(); return err("Komment topilmadi!", 404)
    requester = urow(c, b.user_id)
    if r["user_id"] != b.user_id and not (requester and requester["username"] == "boss"):
        c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM news_comments WHERE id=%s", (b.comment_id,))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/notifications")
def notifications(user_id: int):
    c = db()
    out = []
    for r in c.execute("""SELECT l."timestamp" ts, u.username, u.fullname, p.content snippet FROM likes l
        JOIN posts p ON p.id=l.post_id JOIN users u ON u.id=l.user_id
        WHERE p.user_id=%s AND l.user_id!=%s AND l.is_like=1 ORDER BY l.id DESC LIMIT 10""",
        (user_id, user_id)).fetchall():
        out.append({"type": "like", **dict(r)})
    for r in c.execute("""SELECT m."timestamp" ts, u.username, u.fullname, m.content snippet FROM comments m
        JOIN posts p ON p.id=m.post_id JOIN users u ON u.id=m.user_id
        WHERE p.user_id=%s AND m.user_id!=%s ORDER BY m.id DESC LIMIT 10""",
        (user_id, user_id)).fetchall():
        out.append({"type": "comment", **dict(r)})
    for r in c.execute("""SELECT "timestamp" ts, author username, author fullname, title snippet
        FROM school_news ORDER BY id DESC LIMIT 5""").fetchall():
        out.append({"type": "news", **dict(r)})
    c.close()
    out.sort(key=lambda x: x["ts"] or "", reverse=True)
    return out[:30]

@app.post("/api/admin/news_rights")
def rights(b: RightsReq):
    c = db(); boss = urow(c, b.boss_id)
    if not boss or boss["username"] != "boss": c.close(); return err("Faqat @boss!", 403)
    tg = c.execute("SELECT id,can_post_news FROM users WHERE username=%s", (clean_u(b.target_username),)).fetchone()
    if not tg: c.close(); return err("Topilmadi!", 404)
    nv = 0 if tg["can_post_news"] == 1 else 1
    c.execute("UPDATE users SET can_post_news=%s WHERE id=%s", (nv, tg["id"]))
    c.commit(); c.close(); return {"granted": bool(nv)}

@app.post("/api/admin/delete_user")
def admin_delete_user(b: DeleteUserReq):
    """Boss-only: permanently removes a user and every row that references
    them (posts, comments, likes, follows, certificates, ...) so no orphaned
    data is left behind."""
    c = db(); boss = urow(c, b.boss_id)
    if not boss or boss["username"] != "boss": c.close(); return err("Faqat @boss!", 403)
    tu = clean_u(b.target_username)
    if tu == "boss": c.close(); return err("@boss akkountini o'chirib bo'lmaydi!")
    tg = c.execute("SELECT id FROM users WHERE username=%s", (tu,)).fetchone()
    if not tg: c.close(); return err("Foydalanuvchi topilmadi!", 404)
    uid = tg["id"]
    try:
        post_ids = [r["id"] for r in c.execute("SELECT id FROM posts WHERE user_id=%s", (uid,)).fetchall()]
        for pid in post_ids:
            c.execute("DELETE FROM likes WHERE post_id=%s", (pid,))
            c.execute("DELETE FROM comments WHERE post_id=%s", (pid,))
        c.execute("DELETE FROM posts WHERE user_id=%s", (uid,))
        cm_ids = [r["id"] for r in c.execute("SELECT id FROM comments WHERE user_id=%s", (uid,)).fetchall()]
        for cid in cm_ids:
            c.execute("DELETE FROM comment_likes WHERE comment_id=%s", (cid,))
        c.execute("DELETE FROM comments WHERE user_id=%s", (uid,))
        c.execute("DELETE FROM likes WHERE user_id=%s", (uid,))
        c.execute("DELETE FROM comment_likes WHERE user_id=%s", (uid,))
        c.execute("DELETE FROM bookmarks WHERE user_id=%s", (uid,))
        nc_ids = [r["id"] for r in c.execute("SELECT id FROM news_comments WHERE user_id=%s", (uid,)).fetchall()]
        for ncid in nc_ids:
            c.execute("DELETE FROM news_comment_likes WHERE comment_id=%s", (ncid,))
        c.execute("DELETE FROM news_comments WHERE user_id=%s", (uid,))
        c.execute("DELETE FROM news_likes WHERE user_id=%s", (uid,))
        c.execute("DELETE FROM news_comment_likes WHERE user_id=%s", (uid,))
        c.execute("DELETE FROM certificates WHERE user_id=%s", (uid,))
        c.execute("DELETE FROM follows WHERE follower_id=%s OR following_id=%s", (uid, uid))
        c.execute("DELETE FROM users WHERE id=%s", (uid,))
        c.commit()
    except Exception as e:
        try: c._conn.rollback()
        except Exception: pass
        c.close(); return err(f"O'chirishda xatolik: {e}", 500)
    c.close(); return {"success": True}

@app.post("/api/users/remove_follower")
def remove_follower(b: RemoveFollowerReq):
    """Lets a user forcibly remove someone from their own followers list."""
    c = db()
    fu = c.execute("SELECT id FROM users WHERE username=%s", (clean_u(b.follower_username),)).fetchone()
    if not fu: c.close(); return err("Topilmadi!", 404)
    c.execute("DELETE FROM follows WHERE follower_id=%s AND following_id=%s", (fu["id"], b.owner_id))
    c.commit(); c.close(); return {"success": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
