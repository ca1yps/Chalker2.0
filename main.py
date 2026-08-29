import os
import queue
import re
import threading
import time
import uuid
from typing import Optional

import boto3
from botocore.client import Config as _BotoConfig
import psycopg2
from psycopg2 import pool

from fastapi import FastAPI, Form, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("COCKROACH_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL (yoki COCKROACH_URL) environment variable topilmadi. "
        "Render'da Environment bo'limiga CockroachDB connection string'ini qo'shing."
    )

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_INDEX_CANDIDATES = [
    os.path.join(_BASE_DIR, "templates", "index.html"),
    os.path.join(_BASE_DIR, "index.html"),
]
INDEX = next((p for p in _INDEX_CANDIDATES if os.path.exists(p)), _INDEX_CANDIDATES[-1])

app = FastAPI(title="Chalker")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"])

_POOL_MIN = 1
_POOL_MAX = 10
_pg_pool = None
_pool_lock = threading.Lock()

def get_pool():
    global _pg_pool
    if _pg_pool is None:
        with _pool_lock:
            if _pg_pool is None:
                _pg_pool = pool.ThreadedConnectionPool(
                    minconn=_POOL_MIN,
                    maxconn=_POOL_MAX,
                    dsn=DATABASE_URL
                )
    return _pg_pool

def _format_sql(sql: str) -> str:
    # TOP N ni LIMIT N ga o'girish
    top_match = re.search(r'\bSELECT\s+TOP\s+(\d+)\s+', sql, flags=re.IGNORECASE)
    limit_clause = ""
    if top_match:
        limit_count = top_match.group(1)
        sql = re.sub(r'\bSELECT\s+TOP\s+\d+\s+', 'SELECT ', sql, flags=re.IGNORECASE)
        limit_clause = f" LIMIT {limit_count}"

    sql = re.sub(r'\bRETURNING\s+(\w+)\s+INTO\s+\?', r'RETURNING \1', sql, flags=re.IGNORECASE)
    sql = sql.replace("[timestamp]", "timestamp")
    formatted_sql = sql.replace("?", "%s")

    if limit_clause:
        formatted_sql += limit_clause
    return formatted_sql

class _CursorWrap:
    def __init__(self, cur):
        self._cur = cur

    def _cols(self):
        return [d[0].lower() for d in self._cur.description] if self._cur.description else []

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            return None
        return dict(zip(self._cols(), row))

    def fetchall(self):
        if not self._cur.description:
            return []
        cols = self._cols()
        return [dict(zip(cols, row)) for row in self._cur.fetchall()]

class Conn:
    def __init__(self):
        self._conn = get_pool().getconn()
        self._returned = False
        self._broken = False

    def execute(self, sql, params=()):
        try:
            cur = self._conn.cursor()
            formatted_sql = _format_sql(sql)
            cur.execute(formatted_sql, tuple(params))
            return _CursorWrap(cur)
        except Exception as e:
            try:
                get_pool().putconn(self._conn, close=True)
                self._conn = get_pool().getconn()
                cur = self._conn.cursor()
                formatted_sql = _format_sql(sql)
                cur.execute(formatted_sql, tuple(params))
                return _CursorWrap(cur)
            except Exception as ex:
                self._broken = True
                raise ex

    def execute_insert_id(self, sql, params=()):
        try:
            cur = self._conn.cursor()
            formatted_sql = _format_sql(sql)
            cur.execute(formatted_sql, tuple(params))
            row = cur.fetchone()
            if row:
                return int(row[0])
            return None
        except Exception as e:
            self._broken = True
            raise e

    def commit(self):
        self._conn.commit()

    def close(self):
        if self._returned:
            return
        self._returned = True
        try:
            if not self._broken:
                self._conn.rollback()
            get_pool().putconn(self._conn, close=self._broken)
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
        """CREATE TABLE IF NOT EXISTS users (
            id INT8 PRIMARY KEY DEFAULT unique_rowid(),
            username VARCHAR(255) UNIQUE NOT NULL,
            fullname VARCHAR(255),
            school_class VARCHAR(255),
            school_name VARCHAR(255),
            country VARCHAR(255),
            region VARCHAR(255),
            district VARCHAR(255),
            role VARCHAR(50) DEFAULT 'student',
            birth_date VARCHAR(50),
            hide_birth_date INT2 DEFAULT 0,
            bio TEXT,
            heart_status VARCHAR(50) DEFAULT 'Available',
            avatar_base64 TEXT,
            can_post_news INT2 DEFAULT 0,
            password VARCHAR(255) NOT NULL,
            university VARCHAR(255)
        );""",

        """CREATE TABLE IF NOT EXISTS posts (
            id INT8 PRIMARY KEY DEFAULT unique_rowid(),
            user_id INT8 NOT NULL,
            content TEXT,
            media_base64 TEXT,
            media_type VARCHAR(50),
            timestamp VARCHAR(50) DEFAULT to_char(now() + interval '5 hour', 'YYYY-MM-DD HH24:MI:SS')
        );""",

        """CREATE TABLE IF NOT EXISTS comments (
            id INT8 PRIMARY KEY DEFAULT unique_rowid(),
            post_id INT8 NOT NULL,
            user_id INT8 NOT NULL,
            parent_id INT8,
            content TEXT NOT NULL,
            timestamp VARCHAR(50) DEFAULT to_char(now() + interval '5 hour', 'YYYY-MM-DD HH24:MI:SS')
        );""",

        """CREATE TABLE IF NOT EXISTS likes (
            id INT8 PRIMARY KEY DEFAULT unique_rowid(),
            user_id INT8 NOT NULL,
            post_id INT8 NOT NULL,
            is_like INT2 NOT NULL,
            timestamp VARCHAR(50) DEFAULT to_char(now() + interval '5 hour', 'YYYY-MM-DD HH24:MI:SS')
        );""",

        """CREATE UNIQUE INDEX IF NOT EXISTS iup ON likes(user_id, post_id);""",

        """CREATE TABLE IF NOT EXISTS follows (
            follower_id INT8 NOT NULL,
            following_id INT8 NOT NULL,
            PRIMARY KEY(follower_id, following_id)
        );""",

        """CREATE TABLE IF NOT EXISTS school_news (
            id INT8 PRIMARY KEY DEFAULT unique_rowid(),
            title TEXT NOT NULL,
            author VARCHAR(255),
            timestamp VARCHAR(50) DEFAULT to_char(now() + interval '5 hour', 'YYYY-MM-DD HH24:MI:SS')
        );""",

        """CREATE TABLE IF NOT EXISTS news_likes (
            id INT8 PRIMARY KEY DEFAULT unique_rowid(),
            user_id INT8 NOT NULL,
            news_id INT8 NOT NULL,
            timestamp VARCHAR(50) DEFAULT to_char(now() + interval '5 hour', 'YYYY-MM-DD HH24:MI:SS')
        );""",

        """CREATE UNIQUE INDEX IF NOT EXISTS iun ON news_likes(user_id, news_id);""",

        """CREATE TABLE IF NOT EXISTS news_comments (
            id INT8 PRIMARY KEY DEFAULT unique_rowid(),
            news_id INT8 NOT NULL,
            user_id INT8 NOT NULL,
            content TEXT NOT NULL,
            timestamp VARCHAR(50) DEFAULT to_char(now() + interval '5 hour', 'YYYY-MM-DD HH24:MI:SS')
        );""",

        """CREATE TABLE IF NOT EXISTS comment_likes (
            id INT8 PRIMARY KEY DEFAULT unique_rowid(),
            user_id INT8 NOT NULL,
            comment_id INT8 NOT NULL REFERENCES comments(id) ON DELETE CASCADE,
            timestamp VARCHAR(50) DEFAULT to_char(now() + interval '5 hour', 'YYYY-MM-DD HH24:MI:SS')
        );""",

        """CREATE UNIQUE INDEX IF NOT EXISTS iucl ON comment_likes(user_id, comment_id);""",

        """CREATE TABLE IF NOT EXISTS news_comment_likes (
            id INT8 PRIMARY KEY DEFAULT unique_rowid(),
            user_id INT8 NOT NULL,
            comment_id INT8 NOT NULL REFERENCES news_comments(id) ON DELETE CASCADE,
            timestamp VARCHAR(50) DEFAULT to_char(now() + interval '5 hour', 'YYYY-MM-DD HH24:MI:SS')
        );""",

        """CREATE UNIQUE INDEX IF NOT EXISTS iuncl ON news_comment_likes(user_id, comment_id);""",

        """CREATE TABLE IF NOT EXISTS certificates (
            id INT8 PRIMARY KEY DEFAULT unique_rowid(),
            user_id INT8 NOT NULL,
            title TEXT NOT NULL,
            image_base64 TEXT,
            verified INT2 DEFAULT 1,
            timestamp VARCHAR(50) DEFAULT to_char(now() + interval '5 hour', 'YYYY-MM-DD HH24:MI:SS')
        );""",

        """ALTER TABLE users ADD COLUMN IF NOT EXISTS university VARCHAR(255);"""
    ]

    for stmt in statements:
        try:
            c.execute(stmt)
            c.commit()
        except Exception as e:
            print(f"[init] schema statement failed: {e}")

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
IMAGE_LIMIT_MSG = "Rasm hajmi 8MB dan oshmasligi kerak!"

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

_USERNAME_RE = re.compile(r"^[a-z0-9_.]{5,}$")
_USERNAME_ERR = "Username kamida 5 belgidan iborat bo'lishi kerak!"
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

    new_id = c.execute_insert_id(
        "INSERT INTO users(username,fullname,password) VALUES(?,?,?) RETURNING id",
        (u, fullname.strip(), password)
    )
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
    try:
        if _r2 is None:
            return err("R2 kalitlari topilmadi!", 500)
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
        return err(f"Xatolik: {e}", 500)

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
    sql = """SELECT p.id,p.user_id,p.content,p.media_base64,p.media_type,p.timestamp,
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
    rows = c.execute("""SELECT c.id,c.parent_id,c.content,c.timestamp,u.username,u.fullname,
        u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM comment_likes cl WHERE cl.comment_id=c.id) likes_count,
        (SELECT 1 FROM comment_likes cl WHERE cl.comment_id=c.id AND cl.user_id=?) my_like
        FROM comments c JOIN users u ON u.id=c.user_id
        WHERE c.post_id=? ORDER BY c.id ASC""", (v, post_id)).fetchall()
    c.close(); return [dict(r) for r in rows]

@app.post("/api/comments/like")
def comment_like(b: CommentLikeReq):
    c = db()
    try:
        comm = c.execute("SELECT id FROM comments WHERE id=?", (b.comment_id,)).fetchone()
        if not comm:
            return err("Komment topilmadi!", 404)

        if c.execute("SELECT 1 FROM comment_likes WHERE user_id=? AND comment_id=?", (b.user_id, b.comment_id)).fetchone():
            c.execute("DELETE FROM comment_likes WHERE user_id=? AND comment_id=?", (b.user_id, b.comment_id))
            liked = False
        else:
            c.execute("INSERT INTO comment_likes(user_id,comment_id) VALUES(?,?)", (b.user_id, b.comment_id))
            liked = True

        cnt = c.execute("SELECT COUNT(*) AS cnt FROM comment_likes WHERE comment_id=?", (b.comment_id,)).fetchone()["cnt"]
        c.commit()
        return {"liked": liked, "count": cnt}
    except Exception as e:
        return err(f"Xatolik: {e}", 500)
    finally:
        c.close()

@app.post("/api/comments/delete")
def comment_delete(b: CommentDel):
    c = db()
    r = c.execute("SELECT user_id FROM comments WHERE id=?", (b.comment_id,)).fetchone()
    if not r: c.close(); return err("Komment topilmadi!", 404)
    requester = urow(c, b.user_id)
    if r["user_id"] != b.user_id and not (requester and requester["username"] == "boss"):
        c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM comment_likes WHERE comment_id=?", (b.comment_id,))
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
    rows = c.execute("""SELECT id,username,fullname,avatar_base64,can_post_news,school_name,
        (SELECT 1 FROM follows WHERE follower_id=? AND following_id=users.id) is_following
        FROM users WHERE username LIKE ? OR fullname LIKE ?
        ORDER BY CASE WHEN username LIKE ? THEN 0 ELSE 1 END, username ASC LIMIT 25""",
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
        c.execute("INSERT INTO follows(follower_id,following_id) VALUES(?,?)", (b.follower_id, tg["id"])); f = True
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
    rows = c.execute("""SELECT n.*,
        (SELECT COUNT(*) FROM news_likes l WHERE l.news_id=n.id) likes_count,
        (SELECT COUNT(*) FROM news_comments m WHERE m.news_id=n.id) comments_count,
        (SELECT 1 FROM news_likes l WHERE l.news_id=n.id AND l.user_id=?) my_like
        FROM school_news n ORDER BY n.id DESC LIMIT 20""", (v,)).fetchall()
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
    rows = c.execute("""SELECT m.id,m.content,m.timestamp,u.username,u.fullname,u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM news_comment_likes cl WHERE cl.comment_id=m.id) likes_count,
        (SELECT 1 FROM news_comment_likes cl WHERE cl.comment_id=m.id AND cl.user_id=?) my_like
        FROM news_comments m JOIN users u ON u.id=m.user_id WHERE m.news_id=? ORDER BY m.id ASC""",
        (v, news_id)).fetchall()
    c.close(); return [dict(r) for r in rows]

@app.post("/api/news/comments/like")
def news_comment_like(b: NewsCommentLikeReq):
    c = db()
    try:
        comm = c.execute("SELECT id FROM news_comments WHERE id=?", (b.comment_id,)).fetchone()
        if not comm:
            return err("Komment topilmadi!", 404)

        if c.execute("SELECT 1 FROM news_comment_likes WHERE user_id=? AND comment_id=?", (b.user_id, b.comment_id)).fetchone():
            c.execute("DELETE FROM news_comment_likes WHERE user_id=? AND comment_id=?", (b.user_id, b.comment_id))
            liked = False
        else:
            c.execute("INSERT INTO news_comment_likes(user_id,comment_id) VALUES(?,?)", (b.user_id, b.comment_id))
            liked = True

        cnt = c.execute("SELECT COUNT(*) AS cnt FROM news_comment_likes WHERE comment_id=?", (b.comment_id,)).fetchone()["cnt"]
        c.commit()
        return {"liked": liked, "count": cnt}
    except Exception as e:
        return err(f"Xatolik: {e}", 500)
    finally:
        c.close()

@app.post("/api/news/comments/delete")
def news_comment_delete(b: NewsCommentDel):
    c = db()
    r = c.execute("SELECT user_id FROM news_comments WHERE id=?", (b.comment_id,)).fetchone()
    if not r: c.close(); return err("Komment topilmadi!", 404)
    requester = urow(c, b.user_id)
    if r["user_id"] != b.user_id and not (requester and requester["username"] == "boss"):
        c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM news_comment_likes WHERE comment_id=?", (b.comment_id,))
    c.execute("DELETE FROM news_comments WHERE id=?", (b.comment_id,))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/notifications")
def notifications(user_id: int):
    c = db()
    out = []
    for r in c.execute("""SELECT l.timestamp ts, u.username, u.fullname, p.content snippet FROM likes l
        JOIN posts p ON p.id=l.post_id JOIN users u ON u.id=l.user_id
        WHERE p.user_id=? AND l.user_id!=? AND l.is_like=1 ORDER BY l.id DESC LIMIT 10""",
        (user_id, user_id)).fetchall():
        out.append({"type": "like", **dict(r)})
    for r in c.execute("""SELECT m.timestamp ts, u.username, u.fullname, m.content snippet FROM comments m
        JOIN posts p ON p.id=m.post_id JOIN users u ON u.id=m.user_id
        WHERE p.user_id=? AND m.user_id!=? ORDER BY m.id DESC LIMIT 10""",
        (user_id, user_id)).fetchall():
        out.append({"type": "comment", **dict(r)})
    for r in c.execute("SELECT timestamp ts, author username, author fullname, title snippet FROM school_news ORDER BY id DESC LIMIT 5").fetchall():
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
    c = db()
    fu = c.execute("SELECT id FROM users WHERE username=?", (clean_u(b.follower_username),)).fetchone()
    if not fu: c.close(); return err("Topilmadi!", 404)
    c.execute("DELETE FROM follows WHERE follower_id=? AND following_id=?", (fu["id"], b.owner_id))
    c.commit(); c.close(); return {"success": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
