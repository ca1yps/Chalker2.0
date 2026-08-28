import os
import queue
import re
import threading
import time
import uuid
from typing import Optional

import boto3
from botocore.client import Config as _BotoConfig
import pyodbc

from fastapi import FastAPI, Form, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Database connection (Azure SQL Database, via pyodbc)
# ---------------------------------------------------------------------------
# Set AZURE_SQL_CONNECTIONSTRING in your host's Environment settings (Render
# -> Environment) -- never commit the real password into the repo / git
# history. Example value:
#   DRIVER={ODBC Driver 18 for SQL Server};SERVER=tcp:chalker-server.database.windows.net,1433;
#   DATABASE=free-sql-db-5447952;UID=chalkeradmin;PWD=<parolingiz>;
#   Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;
AZURE_CONN_STR = os.getenv(
    "AZURE_SQL_CONNECTIONSTRING",
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=tcp:chalker-server.database.windows.net,1433;"
    "DATABASE=free-sql-db-5447952;"
    "Uid=chalkeradmin;"
    "Pwd=ilyo$6eey06072009;"  # <-- Shu yerga Azure SQL parolingizni yozing!
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_INDEX_CANDIDATES = [
    os.path.join(_BASE_DIR, "templates", "index.html"),
    os.path.join(_BASE_DIR, "index.html"),
]
INDEX = next((p for p in _INDEX_CANDIDATES if os.path.exists(p)), _INDEX_CANDIDATES[-1])
app = FastAPI(title="Chalker")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------
# pyodbc has no built-in pool like psycopg2.pool.ThreadedConnectionPool, so
# this is a small hand-rolled one: up to POOL_SIZE live connections are kept
# around and reused across requests instead of opening (and leaking, if an
# endpoint errors before closing it) a brand new physical connection to
# Azure SQL on every single API call.
# ---------------------------------------------------------------------------
_POOL_SIZE = 5
_pool = queue.Queue(maxsize=_POOL_SIZE)
_pool_lock = threading.Lock()
_pool_created = 0


def _new_conn():
    return pyodbc.connect(AZURE_CONN_STR, autocommit=False)


def _new_conn_with_retry(retries=6, delay=5):
    """Used only at startup. A free/serverless Azure SQL database can be
    paused and take tens of seconds to resume, which makes the very first
    login attempt fail with '08S01/HYT00 Login timeout expired' and crashes
    the whole app on deploy. Retry a few times with a short delay instead of
    giving up immediately -- this does NOT help if the real cause is a
    firewall blocking Render's IP (that will just fail every attempt and
    still raise after retries are exhausted)."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return pyodbc.connect(AZURE_CONN_STR, autocommit=False)
        except pyodbc.Error as e:
            last_err = e
            if attempt < retries:
                time.sleep(delay)
    raise last_err


def _get_conn():
    global _pool_created
    try:
        return _pool.get_nowait()
    except queue.Empty:
        pass
    with _pool_lock:
        if _pool_created < _POOL_SIZE:
            _pool_created += 1
            return _new_conn()
    # Pool is fully allocated and every connection is currently in use --
    # wait for one to be returned rather than opening an unbounded number
    # of physical connections to Azure SQL.
    return _pool.get()


def _put_conn(conn):
    try:
        _pool.put_nowait(conn)
    except queue.Full:
        try:
            conn.close()
        except Exception:
            pass


class _CursorWrap:
    """Wraps a pyodbc cursor so fetchone()/fetchall() return plain dicts
    keyed by column name -- pyodbc.Row objects only support index/attribute
    access, but the rest of this file was written expecting RealDictCursor
    -style dict rows (r["colname"], dict(r), etc)."""

    def __init__(self, cur):
        self._cur = cur

    def _cols(self):
        return [d[0] for d in self._cur.description] if self._cur.description else []

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            return None
        return dict(zip(self._cols(), row))

    def fetchall(self):
        cols = self._cols()
        return [dict(zip(cols, row)) for row in self._cur.fetchall()]


class Conn:
    """Thin wrapper so the rest of the file can keep using the same
    sqlite3-style pattern: c.execute(sql, params).fetchone()/.fetchall(),
    c.commit(), c.close() -- but talking to Azure SQL underneath via
    pyodbc, using a small pool of reused connections. pyodbc natively
    accepts '?' placeholders, so none of the query strings below needed
    to be rewritten for that."""

    def __init__(self):
        self._conn = _get_conn()
        self._returned = False
        self._broken = False

    def execute(self, sql, params=()):
        try:
            cur = self._conn.cursor()
            cur.execute(sql, tuple(params))
            return _CursorWrap(cur)
        except pyodbc.Error:
            # The pooled connection is stale -- Azure SQL (or a firewall in
            # between) closed it after sitting idle, so the next query on it
            # fails with things like '08S01 Communication link failure'.
            # Drop it and retry once on a brand new connection instead of
            # bubbling up a 500 for something a reconnect fixes.
            try:
                self._conn.close()
            except Exception:
                pass
            try:
                self._conn = _new_conn()
                cur = self._conn.cursor()
                cur.execute(sql, tuple(params))
                return _CursorWrap(cur)
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
                self._conn.close()
            except Exception:
                pass
            return
        try:
            self._conn.rollback()
        except Exception:
            pass
        _put_conn(self._conn)

    def __del__(self):
        # Safety net: if an endpoint throws before calling c.close() (a bug,
        # an unexpected error, etc.) the connection still gets returned to
        # the pool here once the Conn object is garbage-collected, instead
        # of being leaked forever and slowly exhausting Azure SQL's
        # connection quota.
        try:
            self.close()
        except Exception:
            pass


def db():
    return Conn()


def init():
    # Warm up the pool with one connection that retries on failure, so a
    # slow-to-resume Azure SQL database gets a chance to wake up instead of
    # crashing the whole app the moment `uvicorn` imports this module.
    global _pool_created
    if _pool_created == 0:
        with _pool_lock:
            if _pool_created == 0:
                _pool.put_nowait(_new_conn_with_retry())
                _pool_created = 1
    c = db()
    statements = [
        """IF OBJECT_ID('dbo.users','U') IS NULL
        CREATE TABLE users(
          id INT IDENTITY(1,1) PRIMARY KEY,
          username NVARCHAR(255) UNIQUE NOT NULL,
          fullname NVARCHAR(255),
          school_class NVARCHAR(255),
          school_name NVARCHAR(255),
          country NVARCHAR(255),
          region NVARCHAR(255),
          district NVARCHAR(255),
          role NVARCHAR(50) DEFAULT 'student',
          birth_date NVARCHAR(50),
          hide_birth_date INT DEFAULT 0,
          bio NVARCHAR(MAX),
          heart_status NVARCHAR(50) DEFAULT 'Available',
          avatar_base64 NVARCHAR(MAX),
          can_post_news INT DEFAULT 0,
          password NVARCHAR(255) NOT NULL
        )""",
        """IF OBJECT_ID('dbo.posts','U') IS NULL
        CREATE TABLE posts(
          id INT IDENTITY(1,1) PRIMARY KEY,
          user_id INT NOT NULL,
          content NVARCHAR(MAX),
          media_base64 NVARCHAR(MAX),
          media_type NVARCHAR(50),
          [timestamp] NVARCHAR(50) DEFAULT CONVERT(VARCHAR(19), DATEADD(HOUR, 5, GETUTCDATE()), 120)
        )""",
        """IF OBJECT_ID('dbo.comments','U') IS NULL
        CREATE TABLE comments(
          id INT IDENTITY(1,1) PRIMARY KEY,
          post_id INT NOT NULL,
          user_id INT NOT NULL,
          parent_id INT,
          content NVARCHAR(MAX) NOT NULL,
          [timestamp] NVARCHAR(50) DEFAULT CONVERT(VARCHAR(19), DATEADD(HOUR, 5, GETUTCDATE()), 120)
        )""",
        """IF OBJECT_ID('dbo.likes','U') IS NULL
        CREATE TABLE likes(
          id INT IDENTITY(1,1) PRIMARY KEY,
          user_id INT NOT NULL,
          post_id INT NOT NULL,
          is_like INT NOT NULL,
          [timestamp] NVARCHAR(50) DEFAULT CONVERT(VARCHAR(19), DATEADD(HOUR, 5, GETUTCDATE()), 120)
        )""",
        """IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='iup' AND object_id=OBJECT_ID('likes'))
        CREATE UNIQUE INDEX iup ON likes(user_id, post_id)""",
        """IF OBJECT_ID('dbo.follows','U') IS NULL
        CREATE TABLE follows(
          follower_id INT NOT NULL,
          following_id INT NOT NULL,
          PRIMARY KEY(follower_id, following_id)
        )""",
        """IF OBJECT_ID('dbo.school_news','U') IS NULL
        CREATE TABLE school_news(
          id INT IDENTITY(1,1) PRIMARY KEY,
          title NVARCHAR(MAX) NOT NULL,
          author NVARCHAR(255),
          [timestamp] NVARCHAR(50) DEFAULT CONVERT(VARCHAR(19), DATEADD(HOUR, 5, GETUTCDATE()), 120)
        )""",
        """IF OBJECT_ID('dbo.news_likes','U') IS NULL
        CREATE TABLE news_likes(
          id INT IDENTITY(1,1) PRIMARY KEY,
          user_id INT NOT NULL,
          news_id INT NOT NULL,
          [timestamp] NVARCHAR(50) DEFAULT CONVERT(VARCHAR(19), DATEADD(HOUR, 5, GETUTCDATE()), 120)
        )""",
        """IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='iun' AND object_id=OBJECT_ID('news_likes'))
        CREATE UNIQUE INDEX iun ON news_likes(user_id, news_id)""",
        """IF OBJECT_ID('dbo.news_comments','U') IS NULL
        CREATE TABLE news_comments(
          id INT IDENTITY(1,1) PRIMARY KEY,
          news_id INT NOT NULL,
          user_id INT NOT NULL,
          content NVARCHAR(MAX) NOT NULL,
          [timestamp] NVARCHAR(50) DEFAULT CONVERT(VARCHAR(19), DATEADD(HOUR, 5, GETUTCDATE()), 120)
        )""",
        """IF OBJECT_ID('dbo.comment_likes','U') IS NULL
        CREATE TABLE comment_likes(
          id INT IDENTITY(1,1) PRIMARY KEY,
          user_id INT NOT NULL,
          comment_id INT NOT NULL,
          [timestamp] NVARCHAR(50) DEFAULT CONVERT(VARCHAR(19), DATEADD(HOUR, 5, GETUTCDATE()), 120)
        )""",
        """IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='iucl' AND object_id=OBJECT_ID('comment_likes'))
        CREATE UNIQUE INDEX iucl ON comment_likes(user_id, comment_id)""",
        """IF OBJECT_ID('dbo.news_comment_likes','U') IS NULL
        CREATE TABLE news_comment_likes(
          id INT IDENTITY(1,1) PRIMARY KEY,
          user_id INT NOT NULL,
          comment_id INT NOT NULL,
          [timestamp] NVARCHAR(50) DEFAULT CONVERT(VARCHAR(19), DATEADD(HOUR, 5, GETUTCDATE()), 120)
        )""",
        """IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='iuncl' AND object_id=OBJECT_ID('news_comment_likes'))
        CREATE UNIQUE INDEX iuncl ON news_comment_likes(user_id, comment_id)""",
        """IF OBJECT_ID('dbo.certificates','U') IS NULL
        CREATE TABLE certificates(
          id INT IDENTITY(1,1) PRIMARY KEY,
          user_id INT NOT NULL,
          title NVARCHAR(MAX) NOT NULL,
          image_base64 NVARCHAR(MAX),
          verified INT DEFAULT 1,
          [timestamp] NVARCHAR(50) DEFAULT CONVERT(VARCHAR(19), DATEADD(HOUR, 5, GETUTCDATE()), 120)
        )""",
        """IF COL_LENGTH('dbo.users','university') IS NULL
        ALTER TABLE users ADD university NVARCHAR(255)""",
    ]
    # Fix up the [timestamp] DEFAULT on tables that already existed before
    # this fix (they were created with GETDATE(), which on Azure SQL
    # Database returns UTC time -- 5 hours behind O'zbekiston/Tashkent
    # time, which is exactly the "vaqt noto'g'ri" bug this patches). Brand
    # new tables already get the corrected DEFAULT from the CREATE TABLE
    # statements above; this loop repairs tables created before the fix.
    # It's a no-op (guarded by the LIKE '%DATEADD%' check) once a table's
    # default has already been corrected, so it's safe to run on every
    # startup.
    _TS_TABLES = ["posts", "comments", "likes", "school_news", "news_likes",
                  "news_comments", "comment_likes", "news_comment_likes", "certificates"]
    for tbl in _TS_TABLES:
        statements.append(f"""
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id=OBJECT_ID('dbo.{tbl}') AND name='timestamp')
           AND NOT EXISTS (
             SELECT 1 FROM sys.default_constraints dc
             JOIN sys.columns col ON col.object_id=dc.parent_object_id AND col.column_id=dc.parent_column_id
             WHERE dc.parent_object_id=OBJECT_ID('dbo.{tbl}') AND col.name='timestamp' AND dc.definition LIKE '%DATEADD%'
           )
        BEGIN
          DECLARE @cn NVARCHAR(200);
          SELECT @cn = dc.name FROM sys.default_constraints dc
          JOIN sys.columns col ON col.object_id=dc.parent_object_id AND col.column_id=dc.parent_column_id
          WHERE dc.parent_object_id=OBJECT_ID('dbo.{tbl}') AND col.name='timestamp';
          IF @cn IS NOT NULL EXEC('ALTER TABLE {tbl} DROP CONSTRAINT ' + @cn);
          EXEC('ALTER TABLE {tbl} ADD DEFAULT CONVERT(VARCHAR(19), DATEADD(HOUR, 5, GETUTCDATE()), 120) FOR [timestamp]');
        END""")
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

    # One-time data fix: shift already-stored [timestamp] values +5 hours
    # to correct historic rows that were inserted while the DEFAULT still
    # used GETDATE() (Azure SQL Database always runs in UTC, 5 hours behind
    # O'zbekiston/Tashkent time). Guarded by a tiny migrations table so
    # this can never be re-applied on a later restart and shift times
    # twice.
    try:
        c.execute("""IF OBJECT_ID('dbo.schema_migrations','U') IS NULL
            CREATE TABLE schema_migrations(name NVARCHAR(200) PRIMARY KEY, applied_at DATETIME DEFAULT GETUTCDATE())""")
        c.commit()
        already = c.execute(
            "SELECT 1 FROM schema_migrations WHERE name=?", ("ts_tashkent_utc5_fix",)
        ).fetchone()
        if not already:
            for tbl in _TS_TABLES:
                try:
                    c.execute(f"""UPDATE {tbl} SET [timestamp] = CONVERT(VARCHAR(19),
                        DATEADD(HOUR, 5, CONVERT(DATETIME, [timestamp], 120)), 120)
                        WHERE [timestamp] IS NOT NULL AND ISDATE([timestamp]) = 1""")
                    c.commit()
                except Exception as e:
                    print(f"[init] one-time timestamp fix failed for {tbl} (continuing): {e}")
                    try:
                        c._conn.rollback()
                    except Exception:
                        pass
            c.execute("INSERT INTO schema_migrations(name) VALUES(?)", ("ts_tashkent_utc5_fix",))
            c.commit()
    except Exception as e:
        print(f"[init] migrations bookkeeping failed (continuing): {e}")
        try:
            c._conn.rollback()
        except Exception:
            pass

    c.commit(); c.close()


init()


# ---------------------------------------------------------------------------
# Cloudflare R2 (S3-compatible) object storage -- used for ALL images (post
# media, avatars, certificate photos). Text/post data stays in Azure SQL;
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
    return JSONResponse({"error": m}, status_code=s)
def urow(c, uid):
    return c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
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
    user_id: int; content: str = ""; media_base64: Optional[str] = None; media_type: Optional[str] = None
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

@app.get("/ping")
def ping():
    return "OK"

@app.get("/", response_class=HTMLResponse)
def index():
    with open(INDEX, "r", encoding="utf-8") as f:
        html = f.read()
    return HTMLResponse(html, headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/api/check_username")
def check_username(username: str, exclude_id: Optional[int] = None):
    u = clean_u(username)
    if not u:
        return {"available": False}
    c = db()
    q = "SELECT id FROM users WHERE username=?" + (" AND id!=?" if exclude_id else "")
    row = c.execute(q, (u, exclude_id) if exclude_id else (u,)).fetchone()
    c.close()
    return {"available": row is None}

@app.post("/api/register")
def register(username: str = Form(...), fullname: str = Form(...), password: str = Form(...)):
    u = clean_u(username)
    if not u or not password: return err("Username va parol majburiy!")
    c = db()
    if c.execute("SELECT id FROM users WHERE username=?", (u,)).fetchone():
        c.close(); return err("Bu username band!")
    if not valid_username(u):
        c.close(); return err(_USERNAME_ERR)
    if len(password) < 4:
        c.close(); return err("Parol kamida 4 belgi!")
    cur = c.execute("INSERT INTO users(username,fullname,password) OUTPUT INSERTED.id VALUES(?,?,?)", (u, fullname.strip(), password))
    new_id = cur.fetchone()["id"]
    c.commit(); r = urow(c, new_id); c.close()
    return {"user": pub(r)}

@app.post("/api/login")
def login(username: str = Form(...), password: str = Form(...)):
    c = db()
    r = c.execute("SELECT * FROM users WHERE username=? AND password=?", (clean_u(username), password)).fetchone()
    c.close()
    return {"user": pub(r)} if r else err("Username yoki parol xato!", 401)

@app.post("/api/account/update")
def account(user_id: int = Form(...), current_password: str = Form(...),
            new_username: str = Form(""), new_password: str = Form("")):
    c = db()
    r = c.execute("SELECT * FROM users WHERE id=? AND password=?", (user_id, current_password)).fetchone()
    if not r: c.close(); return err("Joriy parol xato!", 401)
    nu = clean_u(new_username)
    if nu and nu != r["username"]:
        if c.execute("SELECT id FROM users WHERE username=?", (nu,)).fetchone():
            c.close(); return err("Bu username band!")
        if not valid_username(nu):
            c.close(); return err(_USERNAME_ERR)
        c.execute("UPDATE users SET username=? WHERE id=?", (nu, user_id))
    if new_password:
        if len(new_password) < 4: c.close(); return err("Yangi parol kamida 4 belgi!")
        c.execute("UPDATE users SET password=? WHERE id=?", (new_password, user_id))
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
    c.execute("""UPDATE users SET fullname=?,school_class=?,school_name=?,country=?,region=?,district=?,
              role=?,bio=?,birth_date=?,hide_birth_date=?,heart_status=?,university=? WHERE id=?""",
              (fullname.strip(), school_class, school_name, country, region, district, role, bio.strip(),
               birth_date, int(hide_birth_date), heart_status, university.strip(), user_id))
    if avatar_base64:
        c.execute("UPDATE users SET avatar_base64=? WHERE id=?", (avatar_base64, user_id))
    c.commit(); r = urow(c, user_id); c.close()
    return {"user": pub(r)} if r else err("Topilmadi!", 404)

@app.get("/api/users/{username}")
def get_user(username: str, viewer_id: Optional[int] = None):
    c = db()
    r = c.execute("SELECT * FROM users WHERE username=?", (clean_u(username),)).fetchone()
    if not r: c.close(); return err("Foydalanuvchi topilmadi!", 404)
    d = pub(r)
    d["followers"] = c.execute("SELECT COUNT(*) AS cnt FROM follows WHERE following_id=?", (r["id"],)).fetchone()["cnt"]
    d["following"] = c.execute("SELECT COUNT(*) AS cnt FROM follows WHERE follower_id=?", (r["id"],)).fetchone()["cnt"]
    d["is_following"] = bool(viewer_id and c.execute(
        "SELECT 1 FROM follows WHERE follower_id=? AND following_id=?", (viewer_id, r["id"])).fetchone())
    if int(d.get("hide_birth_date") or 0) == 1 and (viewer_id is None or int(viewer_id) != r["id"]):
        d["birth_date"] = None
    c.close(); return d

@app.get("/api/users/{username}/followers")
def followers_list(username: str, viewer_id: Optional[int] = None):
    c = db()
    u = c.execute("SELECT id FROM users WHERE username=?", (clean_u(username),)).fetchone()
    if not u: c.close(); return err("Topilmadi!", 404)
    v = viewer_id if viewer_id is not None else -1
    rows = c.execute("""SELECT us.id,us.username,us.fullname,us.avatar_base64,us.can_post_news,us.school_name,
        (SELECT 1 FROM follows WHERE follower_id=? AND following_id=us.id) is_following
        FROM follows f JOIN users us ON us.id=f.follower_id
        WHERE f.following_id=? ORDER BY us.username""", (v, u["id"])).fetchall()
    c.close(); return [{**dict(r), "is_following": bool(r["is_following"])} for r in rows]

@app.get("/api/users/{username}/following")
def following_list(username: str, viewer_id: Optional[int] = None):
    c = db()
    u = c.execute("SELECT id FROM users WHERE username=?", (clean_u(username),)).fetchone()
    if not u: c.close(); return err("Topilmadi!", 404)
    v = viewer_id if viewer_id is not None else -1
    rows = c.execute("""SELECT us.id,us.username,us.fullname,us.avatar_base64,us.can_post_news,us.school_name,
        (SELECT 1 FROM follows WHERE follower_id=? AND following_id=us.id) is_following
        FROM follows f JOIN users us ON us.id=f.following_id
        WHERE f.follower_id=? ORDER BY us.username""", (v, u["id"])).fetchall()
    c.close(); return [{**dict(r), "is_following": bool(r["is_following"])} for r in rows]

@app.get("/api/certificates")
def certificates(username: str):
    c = db()
    u = c.execute("SELECT id FROM users WHERE username=?", (clean_u(username),)).fetchone()
    if not u: c.close(); return []
    rows = c.execute("SELECT * FROM certificates WHERE user_id=? ORDER BY id DESC", (u["id"],)).fetchall()
    c.close(); return [dict(r) for r in rows]

@app.post("/api/certificates/create")
def certificate_create(b: CertCreate):
    c = db(); boss = urow(c, b.boss_id)
    if not boss or boss["username"] != "boss": c.close(); return err("Faqat @boss!", 403)
    if not b.title.strip(): c.close(); return err("Nomi majburiy!")
    tg = c.execute("SELECT id FROM users WHERE username=?", (clean_u(b.target_username),)).fetchone()
    if not tg: c.close(); return err("Topilmadi!", 404)
    c.execute("INSERT INTO certificates(user_id,title,image_base64) VALUES(?,?,?)",
              (tg["id"], b.title.strip(), b.image_base64))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/certificates/delete")
def certificate_delete(b: CertDel):
    c = db(); boss = urow(c, b.boss_id)
    if not boss or boss["username"] != "boss": c.close(); return err("Faqat @boss!", 403)
    c.execute("DELETE FROM certificates WHERE id=?", (b.cert_id,))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/certificates/self_create")
def certificate_self_create(b: CertSelfCreate):
    if not b.title.strip(): return err("Nomi majburiy!")
    c = db()
    if not urow(c, b.user_id): c.close(); return err("Foydalanuvchi topilmadi!", 404)
    c.execute("INSERT INTO certificates(user_id,title,image_base64) VALUES(?,?,?)",
              (b.user_id, b.title.strip(), b.image_base64))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/certificates/self_delete")
def certificate_self_delete(b: CertSelfDel):
    c = db()
    r = c.execute("SELECT user_id FROM certificates WHERE id=?", (b.cert_id,)).fetchone()
    if not r: c.close(); return err("Topilmadi!", 404)
    requester = urow(c, b.user_id)
    if r["user_id"] != b.user_id and not (requester and requester["username"] == "boss"):
        c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM certificates WHERE id=?", (b.cert_id,))
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
    if not b.content.strip() and not b.media_base64: return err("Post bo'sh!")
    c = db()
    c.execute("INSERT INTO posts(user_id,content,media_base64,media_type) VALUES(?,?,?,?)",
              (b.user_id, b.content.strip(), b.media_base64, b.media_type))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/posts/update")
def post_update(b: PostEdit):
    c = db()
    r = c.execute("SELECT user_id FROM posts WHERE id=?", (b.post_id,)).fetchone()
    if not r or r["user_id"] != b.user_id: c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("UPDATE posts SET content=? WHERE id=?", (b.content.strip(), b.post_id))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/posts/delete")
def post_delete(b: PostDel):
    c = db()
    r = c.execute("SELECT user_id FROM posts WHERE id=?", (b.post_id,)).fetchone()
    if not r or r["user_id"] != b.user_id: c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM posts WHERE id=?", (b.post_id,))
    c.execute("DELETE FROM likes WHERE post_id=?", (b.post_id,))
    c.execute("DELETE FROM comments WHERE post_id=?", (b.post_id,))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/posts")
def posts(user_id: Optional[int] = None, author: Optional[str] = None):
    v = user_id if user_id is not None else -1
    c = db()
    sql = """SELECT p.id,p.user_id,p.content,p.media_base64,p.media_type,p.[timestamp],
        u.username,u.fullname,u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM likes l WHERE l.post_id=p.id AND l.is_like=1) likes_count,
        (SELECT COUNT(*) FROM comments cm WHERE cm.post_id=p.id) comments_count,
        (SELECT l.is_like FROM likes l WHERE l.post_id=p.id AND l.user_id=?) my_status
        FROM posts p JOIN users u ON u.id=p.user_id"""
    params = [v]
    if author:
        sql += " WHERE u.username=?"
        params.append(clean_u(author))
    sql += " ORDER BY p.id DESC"
    rows = c.execute(sql, tuple(params)).fetchall()
    c.close(); return [dict(r) for r in rows]

@app.post("/api/posts/like")
def post_like(b: LikeReq):
    c = db()
    r = c.execute("SELECT is_like FROM likes WHERE user_id=? AND post_id=?", (b.user_id, b.post_id)).fetchone()
    if r and r["is_like"] == b.is_like:
        c.execute("DELETE FROM likes WHERE user_id=? AND post_id=?", (b.user_id, b.post_id)); liked = False
    elif r:
        c.execute("UPDATE likes SET is_like=? WHERE user_id=? AND post_id=?", (b.is_like, b.user_id, b.post_id)); liked = True
    else:
        c.execute("INSERT INTO likes(user_id,post_id,is_like) VALUES(?,?,?)", (b.user_id, b.post_id, b.is_like)); liked = True
    c.commit(); c.close(); return {"liked": liked}

@app.post("/api/comments/create")
def comment_create(b: CommentCreate):
    if not b.content.strip(): return err("Komment bo'sh!")
    c = db()
    c.execute("INSERT INTO comments(post_id,user_id,parent_id,content) VALUES(?,?,?,?)",
              (b.post_id, b.user_id, b.parent_id, b.content.strip()))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/comments")
def comments(post_id: int, viewer_id: Optional[int] = None):
    v = viewer_id if viewer_id is not None else -1
    c = db()
    rows = c.execute("""SELECT c.id,c.parent_id,c.content,c.[timestamp],u.username,u.fullname,
        u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM comment_likes cl WHERE cl.comment_id=c.id) likes_count,
        (SELECT 1 FROM comment_likes cl WHERE cl.comment_id=c.id AND cl.user_id=?) my_like
        FROM comments c JOIN users u ON u.id=c.user_id
        WHERE c.post_id=? ORDER BY c.id ASC""", (v, post_id)).fetchall()
    c.close(); return [dict(r) for r in rows]

@app.post("/api/comments/like")
def comment_like(b: CommentLikeReq):
    c = db()
    if c.execute("SELECT 1 FROM comment_likes WHERE user_id=? AND comment_id=?", (b.user_id, b.comment_id)).fetchone():
        c.execute("DELETE FROM comment_likes WHERE user_id=? AND comment_id=?", (b.user_id, b.comment_id)); liked = False
    else:
        c.execute("INSERT INTO comment_likes(user_id,comment_id) VALUES(?,?)", (b.user_id, b.comment_id)); liked = True
    cnt = c.execute("SELECT COUNT(*) cnt FROM comment_likes WHERE comment_id=?", (b.comment_id,)).fetchone()["cnt"]
    c.commit(); c.close(); return {"liked": liked, "count": cnt}

@app.post("/api/comments/delete")
def comment_delete(b: CommentDel):
    c = db()
    r = c.execute("SELECT user_id FROM comments WHERE id=?", (b.comment_id,)).fetchone()
    if not r: c.close(); return err("Komment topilmadi!", 404)
    requester = urow(c, b.user_id)
    if r["user_id"] != b.user_id and not (requester and requester["username"] == "boss"):
        c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM comments WHERE id=? OR parent_id=?", (b.comment_id, b.comment_id))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/search")
def search(q: str = "", viewer_id: Optional[int] = None):
    q = q.strip()
    if not q:
        return []
    c = db()
    like = f"%{q}%"
    v = viewer_id if viewer_id is not None else -1
    rows = c.execute("""SELECT TOP 25 id,username,fullname,avatar_base64,can_post_news,school_name,
        (SELECT 1 FROM follows WHERE follower_id=? AND following_id=users.id) is_following
        FROM users WHERE username LIKE ? OR fullname LIKE ?
        ORDER BY CASE WHEN username LIKE ? THEN 0 ELSE 1 END, username ASC""",
        (v, like, like, q + "%")).fetchall()
    c.close()
    return [{**dict(r), "is_following": bool(r["is_following"])} for r in rows]

@app.post("/api/users/follow")
def follow(b: FollowReq):
    c = db()
    tg = c.execute("SELECT id FROM users WHERE username=?", (clean_u(b.following_username),)).fetchone()
    if not tg: c.close(); return err("Topilmadi!", 404)
    if tg["id"] == b.follower_id: c.close(); return err("O'zingizga follow bosolmaysiz!")
    if c.execute("SELECT 1 FROM follows WHERE follower_id=? AND following_id=?", (b.follower_id, tg["id"])).fetchone():
        c.execute("DELETE FROM follows WHERE follower_id=? AND following_id=?", (b.follower_id, tg["id"])); f = False
    else:
        c.execute("INSERT INTO follows VALUES(?,?)", (b.follower_id, tg["id"])); f = True
    c.commit(); c.close(); return {"following": f}

@app.post("/api/news/create")
def news_create(b: NewsCreate):
    c = db(); r = urow(c, b.user_id)
    if not news_rights(r): c.close(); return err("Huquq yo'q!", 403)
    c.execute("INSERT INTO school_news(title,author) VALUES(?,?)", (b.title.strip(), r["username"]))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/news/update")
def news_update(b: NewsEdit):
    c = db()
    if not news_rights(urow(c, b.user_id)): c.close(); return err("Huquq yo'q!", 403)
    c.execute("UPDATE school_news SET title=? WHERE id=?", (b.title.strip(), b.news_id))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/news/delete")
def news_delete(b: NewsDel):
    c = db()
    if not news_rights(urow(c, b.user_id)): c.close(); return err("Huquq yo'q!", 403)
    c.execute("DELETE FROM school_news WHERE id=?", (b.news_id,))
    c.execute("DELETE FROM news_likes WHERE news_id=?", (b.news_id,))
    c.execute("DELETE FROM news_comments WHERE news_id=?", (b.news_id,))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/news")
def news(user_id: Optional[int] = None):
    v = user_id if user_id is not None else -1
    c = db()
    rows = c.execute("""SELECT TOP 20 n.*,
        (SELECT COUNT(*) FROM news_likes l WHERE l.news_id=n.id) likes_count,
        (SELECT COUNT(*) FROM news_comments m WHERE m.news_id=n.id) comments_count,
        (SELECT 1 FROM news_likes l WHERE l.news_id=n.id AND l.user_id=?) my_like
        FROM school_news n ORDER BY n.id DESC""", (v,)).fetchall()
    c.close(); return [dict(r) for r in rows]

@app.post("/api/news/like")
def news_like(b: NewsLike):
    c = db()
    if c.execute("SELECT 1 FROM news_likes WHERE user_id=? AND news_id=?", (b.user_id, b.news_id)).fetchone():
        c.execute("DELETE FROM news_likes WHERE user_id=? AND news_id=?", (b.user_id, b.news_id)); lk = False
    else:
        c.execute("INSERT INTO news_likes(user_id,news_id) VALUES(?,?)", (b.user_id, b.news_id)); lk = True
    c.commit(); c.close(); return {"liked": lk}

@app.post("/api/news/comments/create")
def news_comment(b: NewsComment):
    if not b.content.strip(): return err("Komment bo'sh!")
    c = db()
    c.execute("INSERT INTO news_comments(news_id,user_id,content) VALUES(?,?,?)",
              (b.news_id, b.user_id, b.content.strip()))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/news/comments")
def news_comments(news_id: int, viewer_id: Optional[int] = None):
    v = viewer_id if viewer_id is not None else -1
    c = db()
    rows = c.execute("""SELECT m.id,m.content,m.[timestamp],u.username,u.fullname,u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM news_comment_likes cl WHERE cl.comment_id=m.id) likes_count,
        (SELECT 1 FROM news_comment_likes cl WHERE cl.comment_id=m.id AND cl.user_id=?) my_like
        FROM news_comments m JOIN users u ON u.id=m.user_id WHERE m.news_id=? ORDER BY m.id ASC""",
        (v, news_id)).fetchall()
    c.close(); return [dict(r) for r in rows]

@app.post("/api/news/comments/like")
def news_comment_like(b: NewsCommentLikeReq):
    c = db()
    if c.execute("SELECT 1 FROM news_comment_likes WHERE user_id=? AND comment_id=?", (b.user_id, b.comment_id)).fetchone():
        c.execute("DELETE FROM news_comment_likes WHERE user_id=? AND comment_id=?", (b.user_id, b.comment_id)); liked = False
    else:
        c.execute("INSERT INTO news_comment_likes(user_id,comment_id) VALUES(?,?)", (b.user_id, b.comment_id)); liked = True
    cnt = c.execute("SELECT COUNT(*) cnt FROM news_comment_likes WHERE comment_id=?", (b.comment_id,)).fetchone()["cnt"]
    c.commit(); c.close(); return {"liked": liked, "count": cnt}

@app.post("/api/news/comments/delete")
def news_comment_delete(b: NewsCommentDel):
    c = db()
    r = c.execute("SELECT user_id FROM news_comments WHERE id=?", (b.comment_id,)).fetchone()
    if not r: c.close(); return err("Komment topilmadi!", 404)
    requester = urow(c, b.user_id)
    if r["user_id"] != b.user_id and not (requester and requester["username"] == "boss"):
        c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM news_comments WHERE id=?", (b.comment_id,))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/notifications")
def notifications(user_id: int):
    c = db()
    out = []
    for r in c.execute("""SELECT TOP 10 l.[timestamp] ts, u.username, u.fullname, p.content snippet FROM likes l
        JOIN posts p ON p.id=l.post_id JOIN users u ON u.id=l.user_id
        WHERE p.user_id=? AND l.user_id!=? AND l.is_like=1 ORDER BY l.id DESC""",
        (user_id, user_id)).fetchall():
        out.append({"type": "like", **dict(r)})
    for r in c.execute("""SELECT TOP 10 m.[timestamp] ts, u.username, u.fullname, m.content snippet FROM comments m
        JOIN posts p ON p.id=m.post_id JOIN users u ON u.id=m.user_id
        WHERE p.user_id=? AND m.user_id!=? ORDER BY m.id DESC""",
        (user_id, user_id)).fetchall():
        out.append({"type": "comment", **dict(r)})
    for r in c.execute("SELECT TOP 5 [timestamp] ts, author username, author fullname, title snippet FROM school_news ORDER BY id DESC").fetchall():
        out.append({"type": "news", **dict(r)})
    c.close()
    out.sort(key=lambda x: x["ts"] or "", reverse=True)
    return out[:30]

@app.post("/api/admin/news_rights")
def rights(b: RightsReq):
    c = db(); boss = urow(c, b.boss_id)
    if not boss or boss["username"] != "boss": c.close(); return err("Faqat @boss!", 403)
    tg = c.execute("SELECT id,can_post_news FROM users WHERE username=?", (clean_u(b.target_username),)).fetchone()
    if not tg: c.close(); return err("Topilmadi!", 404)
    nv = 0 if tg["can_post_news"] == 1 else 1
    c.execute("UPDATE users SET can_post_news=? WHERE id=?", (nv, tg["id"]))
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
    tg = c.execute("SELECT id FROM users WHERE username=?", (tu,)).fetchone()
    if not tg: c.close(); return err("Foydalanuvchi topilmadi!", 404)
    uid = tg["id"]
    try:
        post_ids = [r["id"] for r in c.execute("SELECT id FROM posts WHERE user_id=?", (uid,)).fetchall()]
        for pid in post_ids:
            c.execute("DELETE FROM likes WHERE post_id=?", (pid,))
            c.execute("DELETE FROM comments WHERE post_id=?", (pid,))
        c.execute("DELETE FROM posts WHERE user_id=?", (uid,))
        cm_ids = [r["id"] for r in c.execute("SELECT id FROM comments WHERE user_id=?", (uid,)).fetchall()]
        for cid in cm_ids:
            c.execute("DELETE FROM comment_likes WHERE comment_id=?", (cid,))
        c.execute("DELETE FROM comments WHERE user_id=?", (uid,))
        c.execute("DELETE FROM likes WHERE user_id=?", (uid,))
        c.execute("DELETE FROM comment_likes WHERE user_id=?", (uid,))
        nc_ids = [r["id"] for r in c.execute("SELECT id FROM news_comments WHERE user_id=?", (uid,)).fetchall()]
        for ncid in nc_ids:
            c.execute("DELETE FROM news_comment_likes WHERE comment_id=?", (ncid,))
        c.execute("DELETE FROM news_comments WHERE user_id=?", (uid,))
        c.execute("DELETE FROM news_likes WHERE user_id=?", (uid,))
        c.execute("DELETE FROM news_comment_likes WHERE user_id=?", (uid,))
        c.execute("DELETE FROM certificates WHERE user_id=?", (uid,))
        c.execute("DELETE FROM follows WHERE follower_id=? OR following_id=?", (uid, uid))
        c.execute("DELETE FROM users WHERE id=?", (uid,))
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
    fu = c.execute("SELECT id FROM users WHERE username=?", (clean_u(b.follower_username),)).fetchone()
    if not fu: c.close(); return err("Topilmadi!", 404)
    c.execute("DELETE FROM follows WHERE follower_id=? AND following_id=?", (fu["id"], b.owner_id))
    c.commit(); c.close(); return {"success": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
