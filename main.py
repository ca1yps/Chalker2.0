import os
import re
import threading
import time
import uuid
from typing import Optional

import boto3
from botocore.client import Config as _BotoConfig
import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor

from fastapi import FastAPI, Form, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Database connection (CockroachDB -- Postgres wire-compatible, via psycopg2)
# ---------------------------------------------------------------------------
# Set DATABASE_URL in your host's Environment settings (Render -> Environment)
# -- never commit the real password into the repo / git history. Example
# value (CockroachDB Cloud connection string):
#   postgresql://<user>:<password>@<host>:26257/<db>?sslmode=verify-full
#
# sslmode=verify-full needs the cluster's CA certificate on disk (see the
# "root.crt" explanation in chat) -- psycopg2/libpq looks for it at:
#   Windows: %APPDATA%\postgresql\root.crt
#   Linux/Render: ~/.postgresql/root.crt
# If that file isn't present, every connection attempt fails with an SSL
# verification error, not a credentials error.
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ilyosbek:CAIsL_qC1EfkDeRKwyN98Q@chalkerdb-19950.jxf.gcp-europe-west3.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full",  # <-- Shu yerga CockroachDB connection stringingizni yozing!
)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_INDEX_CANDIDATES = [
    os.path.join(_BASE_DIR, "templates", "index.html"),
    os.path.join(_BASE_DIR, "index.html"),
]
INDEX = next((p for p in _INDEX_CANDIDATES if os.path.exists(p)), _INDEX_CANDIDATES[-1])
app = FastAPI(title="Chalker")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


@app.exception_handler(psycopg2.IntegrityError)
def _integrity_error_handler(request, exc):
    """A foreign key / unique / not-null violation almost always means the
    frontend acted on stale data -- liking/commenting on a post or user
    that was already deleted, a double-click race creating a duplicate,
    etc -- not a real server bug. Surface it as a normal 409 with a
    friendly message instead of a raw 500 traceback."""
    return JSONResponse(
        {"error": "Bu amalni bajarib bo'lmadi: bog'liq ma'lumot topilmadi yoki avval o'chirilgan. Sahifani yangilab qayta urinib ko'ring."},
        status_code=409,
    )


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
    return JSONResponse({"error": m}, status_code=s)
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
        return JSONResponse({"error": str(e)}, status_code=503)

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
    if not b.content.strip() and not b.media_base64: return err("Post bo'sh!")
    c = db()
    c.execute("INSERT INTO posts(user_id,content,media_base64,media_type) VALUES(%s,%s,%s,%s)",
              (b.user_id, b.content.strip(), b.media_base64, b.media_type))
    c.commit(); c.close(); return {"success": True}

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
    c.commit(); c.close(); return {"success": True}

@app.get("/api/posts")
def posts(user_id: Optional[int] = None, author: Optional[str] = None):
    v = user_id if user_id is not None else -1
    c = db()
    sql = """SELECT p.id,p.user_id,p.content,p.media_base64,p.media_type,p."timestamp",
        u.username,u.fullname,u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM likes l WHERE l.post_id=p.id AND l.is_like=1) likes_count,
        (SELECT COUNT(*) FROM comments cm WHERE cm.post_id=p.id) comments_count,
        (SELECT l.is_like FROM likes l WHERE l.post_id=p.id AND l.user_id=%s) my_status
        FROM posts p JOIN users u ON u.id=p.user_id"""
    params = [v]
    if author:
        sql += " WHERE u.username=%s"
        params.append(clean_u(author))
    sql += " ORDER BY p.id DESC"
    rows = c.execute(sql, tuple(params)).fetchall()
    c.close(); return [dict(r) for r in rows]

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
    c.close(); return [dict(r) for r in rows]

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
        FROM users WHERE username LIKE %s OR fullname LIKE %s
        ORDER BY CASE WHEN username LIKE %s THEN 0 ELSE 1 END, username ASC
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
