import os
import re
import threading
import time
import uuid
from typing import Optional, Union, List, Dict, Any

import boto3
from botocore.client import Config as _BotoConfig
import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor

from fastapi import FastAPI, Form, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

# Set DATABASE_URL in your host's Environment settings (Render -> Environment)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@host:26257/defaultdb?sslmode=verify-full",
)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_INDEX_CANDIDATES = [
    os.path.join(_BASE_DIR, "templates", "index.html"),
    os.path.join(_BASE_DIR, "index.html"),
]
INDEX = next((p for p in _INDEX_CANDIDATES if os.path.exists(p)), _INDEX_CANDIDATES[-1])
app = FastAPI(title="Chalker")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(psycopg2.IntegrityError)
def _integrity_error_handler(request, exc):
    """Surfaces foreign key / duplicate / constraint errors as clean 409 responses."""
    return JSONResponse(
        {
            "error": (
                "Bu amalni bajarib bo'lmadi: bog'liq ma'lumot topilmadi yoki avval o'chirilgan."
                " Sahifani yangilab qayta urinib ko'ring."
            )
        },
        status_code=409,
    )

_POOL_SIZE = 5
_pool_lock = threading.Lock()

def _create_pool_with_retry(retries=10, delay=5):
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
            try:
                self._conn.rollback()
            except Exception:
                pass
            raise
        except psycopg2.Error:
            try:
                self._conn.rollback()
            except Exception:
                pass
            try:
                cur = self._conn.cursor(cursor_factory=RealDictCursor)
                cur.execute(sql, tuple(params))
                return cur
            except Exception:
                self._broken = True
                raise

    def commit(self):
        self._conn.commit()

    def close(self):
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
    c.commit()
    c.close()

init()

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

IMAGE_ALLOWED_EXT = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
IMAGE_MAX_SIZE = 8 * 1024 * 1024
IMAGE_LIMIT_MSG = "Rasm hajmi 8MB dan oshmasligi va faqat (jpg, png, gif, webp) formatda bo'lishi kerak!"

def pub(r):
    if not r:
        return None
    d = dict(r)
    d.pop("password", None)
    return d

def err(m, s=400):
    return JSONResponse({"error": m}, status_code=s)

def urow(c, uid):
    try:
        return c.execute("SELECT * FROM users WHERE id=%s", (int(uid),)).fetchone()
    except Exception:
        return None

def news_rights(r):
    return bool(r and (r["username"] == "boss" or r["can_post_news"] == 1))

def clean_u(u):
    return u.strip().lower().lstrip("@") if u else ""

_USERNAME_RE = re.compile(r"^[a-z0-9_.]{5,}$")
_USERNAME_ERR = "Username kamida 5 belgidan iborat bo'lishi va faqat harf, raqam, \"_\" va \".\" belgilaridan tashkil topishi kerak!"

def valid_username(u):
    return bool(_USERNAME_RE.match(u or ""))

def sanitize_ids(obj: Any) -> Any:
    """Recursively converts any integer ID or CockroachDB 64-bit integer > 2^53 - 1
    (or key names ending in _id or equal to id) into strings so JavaScript
    JSON.parse does not truncate or corrupt BigInt IDs."""
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.items():
            if isinstance(v, int) and (
                v > 9007199254740991
                or v < -9007199254740991
                or k in ("id", "post_id", "comment_id", "parent_id", "news_id", "cert_id", "user_id", "follower_id", "following_id")
            ):
                new_dict[k] = str(v)
            else:
                new_dict[k] = sanitize_ids(v)
        return new_dict
    elif isinstance(obj, list):
        return [sanitize_ids(item) for item in obj]
    return obj

class PostCreate(BaseModel):
    user_id: Union[int, str]
    content: str = ""
    media_base64: Optional[str] = None
    media_type: Optional[str] = None

class PostEdit(BaseModel):
    user_id: Union[int, str]
    post_id: Union[int, str]
    content: str = ""

class PostDel(BaseModel):
    user_id: Union[int, str]
    post_id: Union[int, str]

class LikeReq(BaseModel):
    user_id: Union[int, str]
    post_id: Union[int, str]
    is_like: int = 1

class CommentCreate(BaseModel):
    user_id: Union[int, str]
    post_id: Union[int, str]
    content: str
    parent_id: Optional[Union[int, str]] = None

class FollowReq(BaseModel):
    follower_id: Union[int, str]
    following_username: str

class NewsCreate(BaseModel):
    user_id: Union[int, str]
    title: str

class NewsEdit(BaseModel):
    user_id: Union[int, str]
    news_id: Union[int, str]
    title: str

class NewsDel(BaseModel):
    user_id: Union[int, str]
    news_id: Union[int, str]

class NewsLike(BaseModel):
    user_id: Union[int, str]
    news_id: Union[int, str]

class NewsComment(BaseModel):
    user_id: Union[int, str]
    news_id: Union[int, str]
    content: str

class RightsReq(BaseModel):
    boss_id: Union[int, str]
    target_username: str

class DeleteUserReq(BaseModel):
    boss_id: Union[int, str]
    target_username: str

class RemoveFollowerReq(BaseModel):
    owner_id: Union[int, str]
    follower_username: str

class CommentDel(BaseModel):
    user_id: Union[int, str]
    comment_id: Union[int, str]

class NewsCommentDel(BaseModel):
    user_id: Union[int, str]
    comment_id: Union[int, str]

class CommentLikeReq(BaseModel):
    user_id: Union[int, str]
    comment_id: Union[int, str]

class NewsCommentLikeReq(BaseModel):
    user_id: Union[int, str]
    comment_id: Union[int, str]

class CertCreate(BaseModel):
    boss_id: Union[int, str]
    target_username: str
    title: str
    image_base64: Optional[str] = None

class CertDel(BaseModel):
    boss_id: Union[int, str]
    cert_id: Union[int, str]

class CertSelfCreate(BaseModel):
    user_id: Union[int, str]
    title: str
    image_base64: Optional[str] = None

class CertSelfDel(BaseModel):
    user_id: Union[int, str]
    cert_id: Union[int, str]

@app.get("/ping")
def ping():
    return "OK"

@app.get("/ping-db")
def ping_db():
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
def check_username(username: str, exclude_id: Optional[Union[int, str]] = None):
    u = clean_u(username)
    if not u:
        return {"available": False}
    c = db()
    q = "SELECT id FROM users WHERE username=%s" + (" AND id!=%s" if exclude_id else "")
    params = (u, int(exclude_id)) if exclude_id else (u,)
    row = c.execute(q, params).fetchone()
    c.close()
    return {"available": row is None}

@app.post("/api/register")
def register(username: str = Form(...), fullname: str = Form(...), password: str = Form(...)):
    u = clean_u(username)
    if not u or not password:
        return err("Username va parol majburiy!")
    c = db()
    if c.execute("SELECT id FROM users WHERE username=%s", (u,)).fetchone():
        c.close()
        return err("Bu username band!")
    if not valid_username(u):
        c.close()
        return err(_USERNAME_ERR)
    if len(password) < 4:
        c.close()
        return err("Parol kamida 4 belgi!")
    cur = c.execute("INSERT INTO users(username,fullname,password) VALUES(%s,%s,%s) RETURNING id", (u, fullname.strip(), password))
    new_id = cur.fetchone()["id"]
    c.commit()
    r = urow(c, new_id)
    c.close()
    return sanitize_ids({"user": pub(r)})

@app.post("/api/login")
def login(username: str = Form(...), password: str = Form(...)):
    c = db()
    r = c.execute("SELECT * FROM users WHERE username=%s AND password=%s", (clean_u(username), password)).fetchone()
    c.close()
    return sanitize_ids({"user": pub(r)}) if r else err("Username yoki parol xato!", 401)

@app.post("/api/account/update")
def account(user_id: Union[int, str] = Form(...), current_password: str = Form(...),
            new_username: str = Form(""), new_password: str = Form("")):
    uid = int(user_id)
    c = db()
    r = c.execute("SELECT * FROM users WHERE id=%s AND password=%s", (uid, current_password)).fetchone()
    if not r:
        c.close()
        return err("Joriy parol xato!", 401)
    nu = clean_u(new_username)
    if nu and nu != r["username"]:
        if c.execute("SELECT id FROM users WHERE username=%s", (nu,)).fetchone():
            c.close()
            return err("Bu username band!")
        if not valid_username(nu):
            c.close()
            return err(_USERNAME_ERR)
        c.execute("UPDATE users SET username=%s WHERE id=%s", (nu, uid))
    if new_password:
        if len(new_password) < 4:
            c.close()
            return err("Yangi parol kamida 4 belgi!")
        c.execute("UPDATE users SET password=%s WHERE id=%s", (new_password, uid))
    c.commit()
    r = urow(c, uid)
    c.close()
    return sanitize_ids({"user": pub(r)})

@app.post("/api/profile/update")
def profile(user_id: Union[int, str] = Form(...), fullname: str = Form(""), school_class: str = Form(""),
            school_name: str = Form(""), country: str = Form(""), region: str = Form(""),
            district: str = Form(""), role: str = Form("student"), bio: str = Form(""),
            birth_date: str = Form(""), hide_birth_date: int = Form(0),
            heart_status: str = Form("Available"), avatar_base64: str = Form(""),
            university: str = Form("")):
    uid = int(user_id)
    c = db()
    c.execute("""UPDATE users SET fullname=%s,school_class=%s,school_name=%s,country=%s,region=%s,district=%s,
              role=%s,bio=%s,birth_date=%s,hide_birth_date=%s,heart_status=%s,university=%s WHERE id=%s""",
              (fullname.strip(), school_class, school_name, country, region, district, role, bio.strip(),
               birth_date, int(hide_birth_date), heart_status, university.strip(), uid))
    if avatar_base64:
        c.execute("UPDATE users SET avatar_base64=%s WHERE id=%s", (avatar_base64, uid))
    c.commit()
    r = urow(c, uid)
    c.close()
    return sanitize_ids({"user": pub(r)}) if r else err("Topilmadi!", 404)

@app.get("/api/users/{username}")
def get_user(username: str, viewer_id: Optional[Union[int, str]] = None):
    c = db()
    r = c.execute("SELECT * FROM users WHERE username=%s", (clean_u(username),)).fetchone()
    if not r:
        c.close()
        return err("Foydalanuvchi topilmadi!", 404)
    d = pub(r)
    vid = int(viewer_id) if viewer_id is not None else None
    d["followers"] = c.execute("SELECT COUNT(*) AS cnt FROM follows WHERE following_id=%s", (r["id"],)).fetchone()["cnt"]
    d["following"] = c.execute("SELECT COUNT(*) AS cnt FROM follows WHERE follower_id=%s", (r["id"],)).fetchone()["cnt"]
    d["is_following"] = bool(vid and c.execute(
        "SELECT 1 FROM follows WHERE follower_id=%s AND following_id=%s", (vid, r["id"])).fetchone())
    if int(d.get("hide_birth_date") or 0) == 1 and (vid is None or int(vid) != r["id"]):
        d["birth_date"] = None
    c.close()
    return sanitize_ids(d)

@app.get("/api/users/{username}/followers")
def followers_list(username: str, viewer_id: Optional[Union[int, str]] = None):
    c = db()
    u = c.execute("SELECT id FROM users WHERE username=%s", (clean_u(username),)).fetchone()
    if not u:
        c.close()
        return err("Topilmadi!", 404)
    v = int(viewer_id) if viewer_id is not None else -1
    rows = c.execute("""SELECT us.id,us.username,us.fullname,us.avatar_base64,us.can_post_news,us.school_name,
        (SELECT 1 FROM follows WHERE follower_id=%s AND following_id=us.id) is_following
        FROM follows f JOIN users us ON us.id=f.follower_id
        WHERE f.following_id=%s ORDER BY us.username""", (v, u["id"])).fetchall()
    c.close()
    return sanitize_ids([{**dict(r), "is_following": bool(r["is_following"])} for r in rows])

@app.get("/api/users/{username}/following")
def following_list(username: str, viewer_id: Optional[Union[int, str]] = None):
    c = db()
    u = c.execute("SELECT id FROM users WHERE username=%s", (clean_u(username),)).fetchone()
    if not u:
        c.close()
        return err("Topilmadi!", 404)
    v = int(viewer_id) if viewer_id is not None else -1
    rows = c.execute("""SELECT us.id,us.username,us.fullname,us.avatar_base64,us.can_post_news,us.school_name,
        (SELECT 1 FROM follows WHERE follower_id=%s AND following_id=us.id) is_following
        FROM follows f JOIN users us ON us.id=f.following_id
        WHERE f.follower_id=%s ORDER BY us.username""", (v, u["id"])).fetchall()
    c.close()
    return sanitize_ids([{**dict(r), "is_following": bool(r["is_following"])} for r in rows])

@app.get("/api/certificates")
def certificates(username: str):
    c = db()
    u = c.execute("SELECT id FROM users WHERE username=%s", (clean_u(username),)).fetchone()
    if not u:
        c.close()
        return []
    rows = c.execute("SELECT * FROM certificates WHERE user_id=%s ORDER BY id DESC", (u["id"],)).fetchall()
    c.close()
    return sanitize_ids([dict(r) for r in rows])

@app.post("/api/certificates/create")
def certificate_create(b: CertCreate):
    c = db()
    boss = urow(c, int(b.boss_id))
    if not boss or boss["username"] != "boss":
        c.close()
        return err("Faqat @boss!", 403)
    if not b.title.strip():
        c.close()
        return err("Nomi majburiy!")
    tg = c.execute("SELECT id FROM users WHERE username=%s", (clean_u(b.target_username),)).fetchone()
    if not tg:
        c.close()
        return err("Topilmadi!", 404)
    c.execute("INSERT INTO certificates(user_id,title,image_base64) VALUES(%s,%s,%s)",
              (tg["id"], b.title.strip(), b.image_base64))
    c.commit()
    c.close()
    return {"success": True}

@app.post("/api/certificates/delete")
def certificate_delete(b: CertDel):
    c = db()
    boss = urow(c, int(b.boss_id))
    if not boss or boss["username"] != "boss":
        c.close()
        return err("Faqat @boss!", 403)
    c.execute("DELETE FROM certificates WHERE id=%s", (int(b.cert_id),))
    c.commit()
    c.close()
    return {"success": True}

@app.post("/api/certificates/self_create")
def certificate_self_create(b: CertSelfCreate):
    if not b.title.strip():
        return err("Nomi majburiy!")
    uid = int(b.user_id)
    c = db()
    if not urow(c, uid):
        c.close()
        return err("Foydalanuvchi topilmadi!", 404)
    c.execute("INSERT INTO certificates(user_id,title,image_base64) VALUES(%s,%s,%s)",
              (uid, b.title.strip(), b.image_base64))
    c.commit()
    c.close()
    return {"success": True}

@app.post("/api/certificates/self_delete")
def certificate_self_delete(b: CertSelfDel):
    cid = int(b.cert_id)
    uid = int(b.user_id)
    c = db()
    r = c.execute("SELECT user_id FROM certificates WHERE id=%s", (cid,)).fetchone()
    if not r:
        c.close()
        return err("Topilmadi!", 404)
    requester = urow(c, uid)
    if r["user_id"] != uid and not (requester and requester["username"] == "boss"):
        c.close()
        return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM certificates WHERE id=%s", (cid,))
    c.commit()
    c.close()
    return {"success": True}

@app.post("/api/upload/image")
async def upload_image(file: UploadFile = File(...)):
    try:
        if _r2 is None:
            return err("Fayl xizmati sozlanmagan: R2 kalitlari (.env) topilmadi!", 500)
        orig_name = file.filename or "rasm.jpg"
        ext = os.path.splitext(orig_name)[1].lower()
        if ext not in IMAGE_ALLOWED_EXT:
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
    if not b.content.strip() and not b.media_base64:
        return err("Post bo'sh!")
    uid = int(b.user_id)
    c = db()
    c.execute("INSERT INTO posts(user_id,content,media_base64,media_type) VALUES(%s,%s,%s,%s)",
              (uid, b.content.strip(), b.media_base64, b.media_type))
    c.commit()
    c.close()
    return {"success": True}

@app.post("/api/posts/update")
def post_update(b: PostEdit):
    uid = int(b.user_id)
    pid = int(b.post_id)
    c = db()
    r = c.execute("SELECT user_id FROM posts WHERE id=%s", (pid,)).fetchone()
    if not r or r["user_id"] != uid:
        c.close()
        return err("Ruxsat yo'q!", 403)
    c.execute("UPDATE posts SET content=%s WHERE id=%s", (b.content.strip(), pid))
    c.commit()
    c.close()
    return {"success": True}

@app.post("/api/posts/delete")
def post_delete(b: PostDel):
    uid = int(b.user_id)
    pid = int(b.post_id)
    c = db()
    r = c.execute("SELECT user_id FROM posts WHERE id=%s", (pid,)).fetchone()
    if not r or r["user_id"] != uid:
        c.close()
        return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM posts WHERE id=%s", (pid,))
    c.execute("DELETE FROM likes WHERE post_id=%s", (pid,))
    c.execute("DELETE FROM comments WHERE post_id=%s", (pid,))
    c.commit()
    c.close()
    return {"success": True}

@app.get("/api/posts")
def posts(user_id: Optional[Union[int, str]] = None, author: Optional[str] = None):
    v = int(user_id) if user_id is not None and str(user_id).lstrip("-").isdigit() else -1
    c = db()
    sql = """SELECT p.id,p.user_id,p.content,p.media_base64,p.media_type,p."timestamp",
        u.username,u.fullname,u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM likes l WHERE l.post_id=p.id AND l.is_like=1) likes_count,
        (SELECT COUNT(*) FROM comments cm WHERE cm.post_id=p.id) comments_count,
        (SELECT l.is_like FROM likes l WHERE l.post_id=p.id AND l.user_id=%s LIMIT 1) my_status
        FROM posts p JOIN users u ON u.id=p.user_id"""
    params = [v]
    if author:
        sql += " WHERE u.username=%s"
        params.append(clean_u(author))
    sql += " ORDER BY p.id DESC"
    rows = c.execute(sql, tuple(params)).fetchall()
    c.close()
    return JSONResponse(
        sanitize_ids([dict(r) for r in rows]),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"}
    )

@app.post("/api/posts/like")
def post_like(b: LikeReq):
    uid = int(b.user_id)
    pid = int(b.post_id)
    c = db()
    r = c.execute("SELECT is_like FROM likes WHERE user_id=%s AND post_id=%s", (uid, pid)).fetchone()
    if r and r["is_like"] == b.is_like:
        c.execute("DELETE FROM likes WHERE user_id=%s AND post_id=%s", (uid, pid))
        liked = False
    elif r:
        c.execute("UPDATE likes SET is_like=%s WHERE user_id=%s AND post_id=%s", (b.is_like, uid, pid))
        liked = True
    else:
        c.execute("INSERT INTO likes(user_id,post_id,is_like) VALUES(%s,%s,%s)", (uid, pid, b.is_like))
        liked = True
    c.commit()
    c.close()
    return {"liked": liked}

@app.post("/api/comments/create")
def comment_create(b: CommentCreate):
    if not b.content.strip():
        return err("Komment bo'sh!")
    uid = int(b.user_id)
    pid = int(b.post_id)
    prid = int(b.parent_id) if b.parent_id is not None else None
    c = db()
    c.execute("INSERT INTO comments(post_id,user_id,parent_id,content) VALUES(%s,%s,%s,%s)",
              (pid, uid, prid, b.content.strip()))
    c.commit()
    c.close()
    return {"success": True}

@app.get("/api/comments")
def comments(post_id: Union[int, str], viewer_id: Optional[Union[int, str]] = None):
    pid = int(post_id)
    v = int(viewer_id) if viewer_id is not None and str(viewer_id).lstrip("-").isdigit() else -1
    c = db()
    rows = c.execute("""SELECT c.id,c.parent_id,c.content,c."timestamp",u.username,u.fullname,
        u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM comment_likes cl WHERE cl.comment_id=c.id) likes_count,
        (SELECT 1 FROM comment_likes cl WHERE cl.comment_id=c.id AND cl.user_id=%s LIMIT 1) my_like
        FROM comments c JOIN users u ON u.id=c.user_id
        WHERE c.post_id=%s ORDER BY c.id ASC""", (v, pid)).fetchall()
    c.close()
    return JSONResponse(
        sanitize_ids([dict(r) for r in rows]),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"}
    )

@app.post("/api/comments/like")
def comment_like(b: CommentLikeReq):
    uid = int(b.user_id)
    cid = int(b.comment_id)
    c = db()
    if c.execute("SELECT 1 FROM comment_likes WHERE user_id=%s AND comment_id=%s", (uid, cid)).fetchone():
        c.execute("DELETE FROM comment_likes WHERE user_id=%s AND comment_id=%s", (uid, cid))
        liked = False
    else:
        c.execute("INSERT INTO comment_likes(user_id,comment_id) VALUES(%s,%s)", (uid, cid))
        liked = True
    cnt = c.execute("SELECT COUNT(*) cnt FROM comment_likes WHERE comment_id=%s", (cid,)).fetchone()["cnt"]
    c.commit()
    c.close()
    return {"liked": liked, "count": cnt}

@app.post("/api/comments/delete")
def comment_delete(b: CommentDel):
    uid = int(b.user_id)
    cid = int(b.comment_id)
    c = db()
    r = c.execute("SELECT user_id FROM comments WHERE id=%s", (cid,)).fetchone()
    if not r:
        c.close()
        return err("Komment topilmadi!", 404)
    requester = urow(c, uid)
    if r["user_id"] != uid and not (requester and requester["username"] == "boss"):
        c.close()
        return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM comments WHERE id=%s OR parent_id=%s", (cid, cid))
    c.commit()
    c.close()
    return {"success": True}

@app.get("/api/search")
def search(q: str = "", viewer_id: Optional[Union[int, str]] = None):
    q = q.strip()
    if not q:
        return []
    c = db()
    like = f"%{q}%"
    v = int(viewer_id) if viewer_id is not None and str(viewer_id).lstrip("-").isdigit() else -1
    rows = c.execute("""SELECT id,username,fullname,avatar_base64,can_post_news,school_name,
        (SELECT 1 FROM follows WHERE follower_id=%s AND following_id=users.id LIMIT 1) is_following
        FROM users WHERE username LIKE %s OR fullname LIKE %s
        ORDER BY CASE WHEN username LIKE %s THEN 0 ELSE 1 END, username ASC
        LIMIT 25""",
        (v, like, like, q + "%")).fetchall()
    c.close()
    return sanitize_ids([{**dict(r), "is_following": bool(r["is_following"])} for r in rows])

@app.post("/api/users/follow")
def follow(b: FollowReq):
    fid = int(b.follower_id)
    c = db()
    tg = c.execute("SELECT id FROM users WHERE username=%s", (clean_u(b.following_username),)).fetchone()
    if not tg:
        c.close()
        return err("Topilmadi!", 404)
    if tg["id"] == fid:
        c.close()
        return err("O'zingizga follow bosolmaysiz!")
    if c.execute("SELECT 1 FROM follows WHERE follower_id=%s AND following_id=%s", (fid, tg["id"])).fetchone():
        c.execute("DELETE FROM follows WHERE follower_id=%s AND following_id=%s", (fid, tg["id"]))
        f = False
    else:
        c.execute("INSERT INTO follows VALUES(%s,%s)", (fid, tg["id"]))
        f = True
    c.commit()
    c.close()
    return {"following": f}

@app.post("/api/news/create")
def news_create(b: NewsCreate):
    uid = int(b.user_id)
    c = db()
    r = urow(c, uid)
    if not news_rights(r):
        c.close()
        return err("Huquq yo'q!", 403)
    c.execute("INSERT INTO school_news(title,author) VALUES(%s,%s)", (b.title.strip(), r["username"]))
    c.commit()
    c.close()
    return {"success": True}

@app.post("/api/news/update")
def news_update(b: NewsEdit):
    uid = int(b.user_id)
    nid = int(b.news_id)
    c = db()
    if not news_rights(urow(c, uid)):
        c.close()
        return err("Huquq yo'q!", 403)
    c.execute("UPDATE school_news SET title=%s WHERE id=%s", (b.title.strip(), nid))
    c.commit()
    c.close()
    return {"success": True}

@app.post("/api/news/delete")
def news_delete(b: NewsDel):
    uid = int(b.user_id)
    nid = int(b.news_id)
    c = db()
    if not news_rights(urow(c, uid)):
        c.close()
        return err("Huquq yo'q!", 403)
    c.execute("DELETE FROM school_news WHERE id=%s", (nid,))
    c.execute("DELETE FROM news_likes WHERE news_id=%s", (nid,))
    c.execute("DELETE FROM news_comments WHERE news_id=%s", (nid,))
    c.commit()
    c.close()
    return {"success": True}

@app.get("/api/news")
def news(user_id: Optional[Union[int, str]] = None):
    v = int(user_id) if user_id is not None and str(user_id).lstrip("-").isdigit() else -1
    c = db()
    rows = c.execute("""SELECT n.*,
        (SELECT COUNT(*) FROM news_likes l WHERE l.news_id=n.id) likes_count,
        (SELECT COUNT(*) FROM news_comments m WHERE m.news_id=n.id) comments_count,
        (SELECT 1 FROM news_likes l WHERE l.news_id=n.id AND l.user_id=%s LIMIT 1) my_like
        FROM school_news n ORDER BY n.id DESC LIMIT 20""", (v,)).fetchall()
    c.close()
    return JSONResponse(
        sanitize_ids([dict(r) for r in rows]),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"}
    )

@app.post("/api/news/like")
def news_like(b: NewsLike):
    uid = int(b.user_id)
    nid = int(b.news_id)
    c = db()
    if c.execute("SELECT 1 FROM news_likes WHERE user_id=%s AND news_id=%s", (uid, nid)).fetchone():
        c.execute("DELETE FROM news_likes WHERE user_id=%s AND news_id=%s", (uid, nid))
        lk = False
    else:
        c.execute("INSERT INTO news_likes(user_id,news_id) VALUES(%s,%s)", (uid, nid))
        lk = True
    c.commit()
    c.close()
    return {"liked": lk}

@app.post("/api/news/comments/create")
def news_comment(b: NewsComment):
    if not b.content.strip():
        return err("Komment bo'sh!")
    uid = int(b.user_id)
    nid = int(b.news_id)
    c = db()
    c.execute("INSERT INTO news_comments(news_id,user_id,content) VALUES(%s,%s,%s)",
              (nid, uid, b.content.strip()))
    c.commit()
    c.close()
    return {"success": True}

@app.get("/api/news/comments")
def news_comments(news_id: Union[int, str], viewer_id: Optional[Union[int, str]] = None):
    nid = int(news_id)
    v = int(viewer_id) if viewer_id is not None and str(viewer_id).lstrip("-").isdigit() else -1
    c = db()
    rows = c.execute("""SELECT m.id,m.content,m."timestamp",u.username,u.fullname,u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM news_comment_likes cl WHERE cl.comment_id=m.id) likes_count,
        (SELECT 1 FROM news_comment_likes cl WHERE cl.comment_id=m.id AND cl.user_id=%s LIMIT 1) my_like
        FROM news_comments m JOIN users u ON u.id=m.user_id WHERE m.news_id=%s ORDER BY m.id ASC""",
        (v, nid)).fetchall()
    c.close()
    return JSONResponse(
        sanitize_ids([dict(r) for r in rows]),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"}
    )

@app.post("/api/news/comments/like")
def news_comment_like(b: NewsCommentLikeReq):
    uid = int(b.user_id)
    cid = int(b.comment_id)
    c = db()
    if c.execute("SELECT 1 FROM news_comment_likes WHERE user_id=%s AND comment_id=%s", (uid, cid)).fetchone():
        c.execute("DELETE FROM news_comment_likes WHERE user_id=%s AND comment_id=%s", (uid, cid))
        liked = False
    else:
        c.execute("INSERT INTO news_comment_likes(user_id,comment_id) VALUES(%s,%s)", (uid, cid))
        liked = True
    cnt = c.execute("SELECT COUNT(*) cnt FROM news_comment_likes WHERE comment_id=%s", (cid,)).fetchone()["cnt"]
    c.commit()
    c.close()
    return {"liked": liked, "count": cnt}

@app.post("/api/news/comments/delete")
def news_comment_delete(b: NewsCommentDel):
    uid = int(b.user_id)
    cid = int(b.comment_id)
    c = db()
    r = c.execute("SELECT user_id FROM news_comments WHERE id=%s", (cid,)).fetchone()
    if not r:
        c.close()
        return err("Komment topilmadi!", 404)
    requester = urow(c, uid)
    if r["user_id"] != uid and not (requester and requester["username"] == "boss"):
        c.close()
        return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM news_comments WHERE id=%s", (cid,))
    c.commit()
    c.close()
    return {"success": True}

@app.get("/api/notifications")
def notifications(user_id: Union[int, str]):
    uid = int(user_id)
    c = db()
    out = []
    for r in c.execute("""SELECT l."timestamp" ts, u.username, u.fullname, p.content snippet FROM likes l
        JOIN posts p ON p.id=l.post_id JOIN users u ON u.id=l.user_id
        WHERE p.user_id=%s AND l.user_id!=%s AND l.is_like=1 ORDER BY l.id DESC LIMIT 10""",
        (uid, uid)).fetchall():
        out.append({"type": "like", **dict(r)})
    for r in c.execute("""SELECT m."timestamp" ts, u.username, u.fullname, m.content snippet FROM comments m
        JOIN posts p ON p.id=m.post_id JOIN users u ON u.id=m.user_id
        WHERE p.user_id=%s AND m.user_id!=%s ORDER BY m.id DESC LIMIT 10""",
        (uid, uid)).fetchall():
        out.append({"type": "comment", **dict(r)})
    for r in c.execute("""SELECT "timestamp" ts, author username, author fullname, title snippet
        FROM school_news ORDER BY id DESC LIMIT 5""").fetchall():
        out.append({"type": "news", **dict(r)})
    c.close()
    out.sort(key=lambda x: x["ts"] or "", reverse=True)
    return sanitize_ids(out[:30])

@app.post("/api/admin/news_rights")
def rights(b: RightsReq):
    bid = int(b.boss_id)
    c = db()
    boss = urow(c, bid)
    if not boss or boss["username"] != "boss":
        c.close()
        return err("Faqat @boss!", 403)
    tg = c.execute("SELECT id,can_post_news FROM users WHERE username=%s", (clean_u(b.target_username),)).fetchone()
    if not tg:
        c.close()
        return err("Topilmadi!", 404)
    nv = 0 if tg["can_post_news"] == 1 else 1
    c.execute("UPDATE users SET can_post_news=%s WHERE id=%s", (nv, tg["id"]))
    c.commit()
    c.close()
    return {"granted": bool(nv)}

@app.post("/api/admin/delete_user")
def admin_delete_user(b: DeleteUserReq):
    bid = int(b.boss_id)
    c = db()
    boss = urow(c, bid)
    if not boss or boss["username"] != "boss":
        c.close()
        return err("Faqat @boss!", 403)
    tu = clean_u(b.target_username)
    if tu == "boss":
        c.close()
        return err("@boss akkountini o'chirib bo'lmaydi!")
    tg = c.execute("SELECT id FROM users WHERE username=%s", (tu,)).fetchone()
    if not tg:
        c.close()
        return err("Foydalanuvchi topilmadi!", 404)
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
        try:
            c._conn.rollback()
        except Exception:
            pass
        c.close()
        return err(f"O'chirishda xatolik: {e}", 500)
    c.close()
    return {"success": True}

@app.post("/api/users/remove_follower")
def remove_follower(b: RemoveFollowerReq):
    oid = int(b.owner_id)
    c = db()
    fu = c.execute("SELECT id FROM users WHERE username=%s", (clean_u(b.follower_username),)).fetchone()
    if not fu:
        c.close()
        return err("Topilmadi!", 404)
    c.execute("DELETE FROM follows WHERE follower_id=%s AND following_id=%s", (fu["id"], oid))
    c.commit()
    c.close()
    return {"success": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
