import psycopg2
import psycopg2.extras
import os
from typing import Optional
from fastapi import FastAPI, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

# Supabase PostgreSQL Ulanish Manzili
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres.ybipdrsxxnikvefpdqx:Ilyo$6eey06072009@aws-0-eu-west-3.pooler.supabase.com:6543/postgres"
)

INDEX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "index.html")
app = FastAPI(title="Chalker")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

def db():
    return psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def init():
    try:
        c = db()
        cur = c.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
          id SERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL,
          fullname TEXT, school_class TEXT, school_name TEXT, country TEXT, region TEXT, district TEXT,
          role TEXT DEFAULT 'student', birth_date TEXT, hide_birth_date INTEGER DEFAULT 0, bio TEXT,
          heart_status TEXT DEFAULT 'Available', avatar_base64 TEXT, can_post_news INTEGER DEFAULT 0, password TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS posts(
          id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL,
          content TEXT, media_base64 TEXT, media_type TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS comments(
          id SERIAL PRIMARY KEY, post_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL, parent_id INTEGER, content TEXT NOT NULL, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS likes(
          id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL,
          post_id INTEGER NOT NULL, is_like INTEGER NOT NULL, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT iup UNIQUE (user_id, post_id)
        );
        CREATE TABLE IF NOT EXISTS chat_messages(
          id SERIAL PRIMARY KEY, sender_id INTEGER NOT NULL,
          receiver_id INTEGER NOT NULL, message TEXT NOT NULL, image_base64 TEXT,
          is_read INTEGER DEFAULT 0, edited INTEGER DEFAULT 0, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS follows(
          follower_id INTEGER NOT NULL, following_id INTEGER NOT NULL,
          PRIMARY KEY(follower_id, following_id)
        );
        CREATE TABLE IF NOT EXISTS school_news(
          id SERIAL PRIMARY KEY, title TEXT NOT NULL,
          author TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS news_likes(
          id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL,
          news_id INTEGER NOT NULL, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT iun UNIQUE (user_id, news_id)
        );
        CREATE TABLE IF NOT EXISTS news_comments(
          id SERIAL PRIMARY KEY, news_id INTEGER NOT NULL,
          user_id INTEGER NOT NULL, content TEXT NOT NULL, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS ceo_page(
          id INTEGER PRIMARY KEY, name TEXT, title TEXT, bio TEXT,
          telegram TEXT, instagram TEXT, chalker TEXT
        );
        CREATE TABLE IF NOT EXISTS ceo_extra(
          id SERIAL PRIMARY KEY, name TEXT, title TEXT, bio TEXT,
          telegram TEXT, instagram TEXT, chalker TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS image_base64 TEXT;
        ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS is_read INTEGER DEFAULT 0;
        ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS edited INTEGER DEFAULT 0;
        """)

        cur.execute("SELECT id FROM ceo_page WHERE id=1")
        if not cur.fetchone():
            cur.execute("""INSERT INTO ceo_page VALUES(1,%s,%s,%s,%s,%s,%s)""",
                        ("Ilyosbek Siddiqjonov", "CEO Founder",
                         "Chalker — o'quvchilar va o'qituvchilar uchun zamonaviy maktab ijtimoiy tarmog'i.",
                         "@ilyos6ee", "@ilyos6ee", "@boss"))
        c.commit()
        cur.close()
        c.close()
    except Exception as e:
        print("DATABASE INIT ERROR:", e)

init()

def pub(r):
    if not r: return None
    d = dict(r)
    d.pop("password", None)
    return d

def err(m, s=400):
    return JSONResponse({"error": m}, status_code=s)

def urow(c, uid):
    cur = c.cursor()
    cur.execute("SELECT * FROM users WHERE id=%s", (uid,))
    res = cur.fetchone()
    cur.close()
    return res

def news_rights(r):
    return bool(r and (r["username"] == "boss" or r["can_post_news"] == 1))

def clean_u(u):
    return u.strip().lower().lstrip("@")

# Models
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
class ChatSend(BaseModel):
    sender_id: int; receiver_username: str; message: str = ""; image_base64: Optional[str] = None
class ChatEdit(BaseModel):
    user_id: int; message_id: int; message: str
class ChatDel(BaseModel):
    user_id: int; message_id: int
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
class CeoUpd(BaseModel):
    user_id: int; name: str; title: str; bio: str; telegram: str; instagram: str; chalker: str
class CeoCreate(BaseModel):
    user_id: int; name: str; title: str = ""; bio: str = ""; telegram: str = ""; instagram: str = ""; chalker: str = ""
class CeoExtraUpd(BaseModel):
    user_id: int; ceo_id: int; name: str; title: str = ""; bio: str = ""; telegram: str = ""; instagram: str = ""; chalker: str = ""
class CeoExtraDel(BaseModel):
    user_id: int; ceo_id: int
class CommentDel(BaseModel):
    user_id: int; comment_id: int
class NewsCommentDel(BaseModel):
    user_id: int; comment_id: int

@app.get("/", response_class=HTMLResponse)
def index():
    with open(INDEX, "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/check_username")
def check_username(username: str, exclude_id: Optional[int] = None):
    u = clean_u(username)
    if not u: return {"available": False}
    c = db(); cur = c.cursor()
    q = "SELECT id FROM users WHERE username=%s" + (" AND id!=%s" if exclude_id else "")
    cur.execute(q, (u, exclude_id) if exclude_id else (u,))
    row = cur.fetchone()
    cur.close(); c.close()
    return {"available": row is None}

@app.post("/api/register")
def register(username: str = Form(...), fullname: str = Form(...), password: str = Form(...)):
    u = clean_u(username)
    if not u or not password: return err("Username va parol majburiy!")
    if len(password) < 4: return err("Parol kamida 4 belgi!")
    c = db(); cur = c.cursor()
    cur.execute("SELECT id FROM users WHERE username=%s", (u,))
    if cur.fetchone():
        cur.close(); c.close(); return err("Bu username band!")
    cur.execute("INSERT INTO users(username,fullname,password) VALUES(%s,%s,%s) RETURNING id", (u, fullname.strip(), password))
    new_id = cur.fetchone()["id"]
    c.commit()
    r = urow(c, new_id)
    cur.close(); c.close()
    return {"user": pub(r)}

@app.post("/api/login")
def login(username: str = Form(...), password: str = Form(...)):
    c = db(); cur = c.cursor()
    cur.execute("SELECT * FROM users WHERE username=%s AND password=%s", (clean_u(username), password))
    r = cur.fetchone()
    cur.close(); c.close()
    return {"user": pub(r)} if r else err("Username yoki parol xato!", 401)

@app.post("/api/account/update")
def account(user_id: int = Form(...), current_password: str = Form(...),
            new_username: str = Form(""), new_password: str = Form("")):
    c = db(); cur = c.cursor()
    cur.execute("SELECT * FROM users WHERE id=%s AND password=%s", (user_id, current_password))
    r = cur.fetchone()
    if not r: cur.close(); c.close(); return err("Joriy parol xato!", 401)
    nu = clean_u(new_username)
    if nu and nu != r["username"]:
        cur.execute("SELECT id FROM users WHERE username=%s", (nu,))
        if cur.fetchone():
            cur.close(); c.close(); return err("Bu username band!")
        cur.execute("UPDATE users SET username=%s WHERE id=%s", (nu, user_id))
    if new_password:
        if len(new_password) < 4: cur.close(); c.close(); return err("Yangi parol kamida 4 belgi!")
        cur.execute("UPDATE users SET password=%s WHERE id=%s", (new_password, user_id))
    c.commit()
    r = urow(c, user_id)
    cur.close(); c.close()
    return {"user": pub(r)}

@app.post("/api/profile/update")
def profile(user_id: int = Form(...), fullname: str = Form(""), school_class: str = Form(""),
            school_name: str = Form(""), country: str = Form(""), region: str = Form(""),
            district: str = Form(""), role: str = Form("student"), bio: str = Form(""),
            birth_date: str = Form(""), hide_birth_date: int = Form(0),
            heart_status: str = Form("Available"), avatar_base64: str = Form("")):
    c = db(); cur = c.cursor()
    cur.execute("""UPDATE users SET fullname=%s,school_class=%s,school_name=%s,country=%s,region=%s,district=%s,
              role=%s,bio=%s,birth_date=%s,hide_birth_date=%s,heart_status=%s WHERE id=%s""",
              (fullname.strip(), school_class, school_name, country, region, district, role, bio.strip(),
               birth_date, int(hide_birth_date), heart_status, user_id))
    if avatar_base64:
        cur.execute("UPDATE users SET avatar_base64=%s WHERE id=%s", (avatar_base64, user_id))
    c.commit()
    r = urow(c, user_id)
    cur.close(); c.close()
    return {"user": pub(r)} if r else err("Topilmadi!", 404)

@app.get("/api/users/{username}")
def get_user(username: str, viewer_id: Optional[int] = None):
    c = db(); cur = c.cursor()
    cur.execute("SELECT * FROM users WHERE username=%s", (clean_u(username),))
    r = cur.fetchone()
    if not r: cur.close(); c.close(); return err("Foydalanuvchi topilmadi!", 404)
    d = pub(r)
    cur.execute("SELECT COUNT(*) as count FROM follows WHERE following_id=%s", (r["id"],))
    d["followers"] = cur.fetchone()["count"]
    cur.execute("SELECT COUNT(*) as count FROM follows WHERE follower_id=%s", (r["id"],))
    d["following"] = cur.fetchone()["count"]
    
    is_f = False
    if viewer_id:
        cur.execute("SELECT 1 FROM follows WHERE follower_id=%s AND following_id=%s", (viewer_id, r["id"]))
        is_f = cur.fetchone() is not None
    d["is_following"] = is_f

    if int(d.get("hide_birth_date") or 0) == 1 and (viewer_id is None or int(viewer_id) != r["id"]):
        d["birth_date"] = None
    cur.close(); c.close()
    return d

@app.post("/api/posts/create")
def post_create(b: PostCreate):
    if not b.content.strip() and not b.media_base64: return err("Post bo'sh!")
    c = db(); cur = c.cursor()
    cur.execute("INSERT INTO posts(user_id,content,media_base64,media_type) VALUES(%s,%s,%s,%s)",
              (b.user_id, b.content.strip(), b.media_base64, b.media_type))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.post("/api/posts/update")
def post_update(b: PostEdit):
    c = db(); cur = c.cursor()
    cur.execute("SELECT user_id FROM posts WHERE id=%s", (b.post_id,))
    r = cur.fetchone()
    if not r or r["user_id"] != b.user_id: cur.close(); c.close(); return err("Ruxsat yo'q!", 403)
    cur.execute("UPDATE posts SET content=%s WHERE id=%s", (b.content.strip(), b.post_id))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.post("/api/posts/delete")
def post_delete(b: PostDel):
    c = db(); cur = c.cursor()
    cur.execute("SELECT user_id FROM posts WHERE id=%s", (b.post_id,))
    r = cur.fetchone()
    if not r or r["user_id"] != b.user_id: cur.close(); c.close(); return err("Ruxsat yo'q!", 403)
    cur.execute("DELETE FROM posts WHERE id=%s", (b.post_id,))
    cur.execute("DELETE FROM likes WHERE post_id=%s", (b.post_id,))
    cur.execute("DELETE FROM comments WHERE post_id=%s", (b.post_id,))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.get("/api/posts")
def posts(user_id: Optional[int] = None):
    v = user_id if user_id is not None else -1
    c = db(); cur = c.cursor()
    cur.execute("""SELECT p.id,p.user_id,p.content,p.media_base64,p.media_type,p.timestamp,
        u.username,u.fullname,u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM likes l WHERE l.post_id=p.id AND l.is_like=1) likes_count,
        (SELECT COUNT(*) FROM comments cm WHERE cm.post_id=p.id) comments_count,
        (SELECT l.is_like FROM likes l WHERE l.post_id=p.id AND l.user_id=%s) my_status
        FROM posts p JOIN users u ON u.id=p.user_id ORDER BY p.id DESC""", (v,))
    rows = cur.fetchall()
    cur.close(); c.close()
    return [dict(r) for r in rows]

@app.post("/api/posts/like")
def post_like(b: LikeReq):
    c = db(); cur = c.cursor()
    cur.execute("SELECT is_like FROM likes WHERE user_id=%s AND post_id=%s", (b.user_id, b.post_id))
    r = cur.fetchone()
    if r and r["is_like"] == b.is_like:
        cur.execute("DELETE FROM likes WHERE user_id=%s AND post_id=%s", (b.user_id, b.post_id))
        liked = False
    else:
        cur.execute("""INSERT INTO likes(user_id,post_id,is_like) VALUES(%s,%s,%s)
                    ON CONFLICT (user_id, post_id) DO UPDATE SET is_like = EXCLUDED.is_like""",
                  (b.user_id, b.post_id, b.is_like))
        liked = True
    c.commit(); cur.close(); c.close()
    return {"liked": liked}

@app.post("/api/comments/create")
def comment_create(b: CommentCreate):
    if not b.content.strip(): return err("Komment bo'sh!")
    c = db(); cur = c.cursor()
    cur.execute("INSERT INTO comments(post_id,user_id,parent_id,content) VALUES(%s,%s,%s,%s)",
              (b.post_id, b.user_id, b.parent_id, b.content.strip()))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.get("/api/comments")
def comments(post_id: int):
    c = db(); cur = c.cursor()
    cur.execute("""SELECT c.id,c.parent_id,c.content,c.timestamp,u.username,u.fullname,
        u.avatar_base64,u.can_post_news FROM comments c JOIN users u ON u.id=c.user_id
        WHERE c.post_id=%s ORDER BY c.id ASC""", (post_id,))
    rows = cur.fetchall()
    cur.close(); c.close()
    return [dict(r) for r in rows]

@app.post("/api/comments/delete")
def comment_delete(b: CommentDel):
    c = db(); cur = c.cursor()
    cur.execute("SELECT user_id FROM comments WHERE id=%s", (b.comment_id,))
    r = cur.fetchone()
    if not r: cur.close(); c.close(); return err("Komment topilmadi!", 404)
    requester = urow(c, b.user_id)
    if r["user_id"] != b.user_id and not (requester and requester["username"] == "boss"):
        cur.close(); c.close(); return err("Ruxsat yo'q!", 403)
    cur.execute("DELETE FROM comments WHERE id=%s OR parent_id=%s", (b.comment_id, b.comment_id))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.post("/api/chat/send")
def chat_send(b: ChatSend):
    if not b.message.strip() and not b.image_base64: return err("Xabar bo'sh!")
    c = db(); cur = c.cursor()
    cur.execute("SELECT id FROM users WHERE username=%s", (clean_u(b.receiver_username),))
    r = cur.fetchone()
    if not r: cur.close(); c.close(); return err("Qabul qiluvchi topilmadi!", 404)
    cur.execute("INSERT INTO chat_messages(sender_id,receiver_id,message,image_base64) VALUES(%s,%s,%s,%s)",
              (b.sender_id, r["id"], b.message.strip(), b.image_base64))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.post("/api/chat/edit")
def chat_edit(b: ChatEdit):
    if not b.message.strip(): return err("Xabar bo'sh!")
    c = db(); cur = c.cursor()
    cur.execute("SELECT sender_id FROM chat_messages WHERE id=%s", (b.message_id,))
    r = cur.fetchone()
    if not r: cur.close(); c.close(); return err("Xabar topilmadi!", 404)
    if r["sender_id"] != b.user_id: cur.close(); c.close(); return err("Ruxsat yo'q!", 403)
    cur.execute("UPDATE chat_messages SET message=%s,edited=1 WHERE id=%s", (b.message.strip(), b.message_id))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.post("/api/chat/delete")
def chat_delete(b: ChatDel):
    c = db(); cur = c.cursor()
    cur.execute("SELECT sender_id FROM chat_messages WHERE id=%s", (b.message_id,))
    r = cur.fetchone()
    if not r: cur.close(); c.close(); return err("Xabar topilmadi!", 404)
    if r["sender_id"] != b.user_id: cur.close(); c.close(); return err("Ruxsat yo'q!", 403)
    cur.execute("DELETE FROM chat_messages WHERE id=%s", (b.message_id,))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.get("/api/chat/history")
def chat_history(user_id: int, partner_username: str):
    c = db(); cur = c.cursor()
    cur.execute("SELECT id,username,fullname,avatar_base64,can_post_news FROM users WHERE username=%s",
              (clean_u(partner_username),))
    p = cur.fetchone()
    if not p: cur.close(); c.close(); return err("Foydalanuvchi topilmadi!", 404)
    cur.execute("UPDATE chat_messages SET is_read=1 WHERE sender_id=%s AND receiver_id=%s AND is_read=0",
              (p["id"], user_id))
    c.commit()
    cur.execute("""SELECT * FROM chat_messages WHERE (sender_id=%s AND receiver_id=%s)
        OR (sender_id=%s AND receiver_id=%s) ORDER BY id ASC""",
        (user_id, p["id"], p["id"], user_id))
    rows = cur.fetchall()
    cur.close(); c.close()
    return {"partner": dict(p), "messages": [dict(r) for r in rows]}

@app.get("/api/chat/active_users")
def chat_users(user_id: int):
    c = db(); cur = c.cursor()
    cur.execute("""SELECT u.id,u.username,u.fullname,u.avatar_base64,u.can_post_news,
        (SELECT message FROM chat_messages m WHERE (m.sender_id=u.id AND m.receiver_id=%s)
         OR (m.sender_id=%s AND m.receiver_id=u.id) ORDER BY m.id DESC LIMIT 1) last_message,
        (SELECT COUNT(*) FROM chat_messages m WHERE m.sender_id=u.id AND m.receiver_id=%s AND m.is_read=0) unread_count
        FROM users u WHERE u.id IN (SELECT sender_id FROM chat_messages WHERE receiver_id=%s
        UNION SELECT receiver_id FROM chat_messages WHERE sender_id=%s) AND u.id!=%s""",
        (user_id, user_id, user_id, user_id, user_id, user_id))
    rows = cur.fetchall()
    cur.close(); c.close()
    return [dict(r) for r in rows]

@app.post("/api/users/follow")
def follow(b: FollowReq):
    c = db(); cur = c.cursor()
    cur.execute("SELECT id FROM users WHERE username=%s", (clean_u(b.following_username),))
    tg = cur.fetchone()
    if not tg: cur.close(); c.close(); return err("Topilmadi!", 404)
    if tg["id"] == b.follower_id: cur.close(); c.close(); return err("O'zingizga follow bosolmaysiz!")
    
    cur.execute("SELECT 1 FROM follows WHERE follower_id=%s AND following_id=%s", (b.follower_id, tg["id"]))
    if cur.fetchone():
        cur.execute("DELETE FROM follows WHERE follower_id=%s AND following_id=%s", (b.follower_id, tg["id"]))
        f = False
    else:
        cur.execute("INSERT INTO follows VALUES(%s,%s)", (b.follower_id, tg["id"]))
        f = True
    c.commit(); cur.close(); c.close()
    return {"following": f}

@app.post("/api/news/create")
def news_create(b: NewsCreate):
    c = db(); cur = c.cursor()
    r = urow(c, b.user_id)
    if not news_rights(r): cur.close(); c.close(); return err("Huquq yo'q!", 403)
    cur.execute("INSERT INTO school_news(title,author) VALUES(%s,%s)", (b.title.strip(), r["username"]))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.post("/api/news/update")
def news_update(b: NewsEdit):
    c = db(); cur = c.cursor()
    if not news_rights(urow(c, b.user_id)): cur.close(); c.close(); return err("Huquq yo'q!", 403)
    cur.execute("UPDATE school_news SET title=%s WHERE id=%s", (b.title.strip(), b.news_id))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.post("/api/news/delete")
def news_delete(b: NewsDel):
    c = db(); cur = c.cursor()
    if not news_rights(urow(c, b.user_id)): cur.close(); c.close(); return err("Huquq yo'q!", 403)
    cur.execute("DELETE FROM school_news WHERE id=%s", (b.news_id,))
    cur.execute("DELETE FROM news_likes WHERE news_id=%s", (b.news_id,))
    cur.execute("DELETE FROM news_comments WHERE news_id=%s", (b.news_id,))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.get("/api/news")
def news(user_id: Optional[int] = None):
    v = user_id if user_id is not None else -1
    c = db(); cur = c.cursor()
    cur.execute("""SELECT n.*,
        (SELECT COUNT(*) FROM news_likes l WHERE l.news_id=n.id) likes_count,
        (SELECT COUNT(*) FROM news_comments m WHERE m.news_id=n.id) comments_count,
        (SELECT 1 FROM news_likes l WHERE l.news_id=n.id AND l.user_id=%s) my_like
        FROM school_news n ORDER BY n.id DESC LIMIT 20""", (v,))
    rows = cur.fetchall()
    cur.close(); c.close()
    return [dict(r) for r in rows]

@app.post("/api/news/like")
def news_like(b: NewsLike):
    c = db(); cur = c.cursor()
    cur.execute("SELECT 1 FROM news_likes WHERE user_id=%s AND news_id=%s", (b.user_id, b.news_id))
    if cur.fetchone():
        cur.execute("DELETE FROM news_likes WHERE user_id=%s AND news_id=%s", (b.user_id, b.news_id))
        lk = False
    else:
        cur.execute("INSERT INTO news_likes(user_id,news_id) VALUES(%s,%s)", (b.user_id, b.news_id))
        lk = True
    c.commit(); cur.close(); c.close()
    return {"liked": lk}

@app.post("/api/news/comments/create")
def news_comment(b: NewsComment):
    if not b.content.strip(): return err("Komment bo'sh!")
    c = db(); cur = c.cursor()
    cur.execute("INSERT INTO news_comments(news_id,user_id,content) VALUES(%s,%s,%s)",
              (b.news_id, b.user_id, b.content.strip()))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.get("/api/news/comments")
def news_comments(news_id: int):
    c = db(); cur = c.cursor()
    cur.execute("""SELECT m.id,m.content,m.timestamp,u.username,u.fullname,u.avatar_base64,u.can_post_news
        FROM news_comments m JOIN users u ON u.id=m.user_id WHERE m.news_id=%s ORDER BY m.id ASC""",
        (news_id,))
    rows = cur.fetchall()
    cur.close(); c.close()
    return [dict(r) for r in rows]

@app.post("/api/news/comments/delete")
def news_comment_delete(b: NewsCommentDel):
    c = db(); cur = c.cursor()
    cur.execute("SELECT user_id FROM news_comments WHERE id=%s", (b.comment_id,))
    r = cur.fetchone()
    if not r: cur.close(); c.close(); return err("Komment topilmadi!", 404)
    requester = urow(c, b.user_id)
    if r["user_id"] != b.user_id and not (requester and requester["username"] == "boss"):
        cur.close(); c.close(); return err("Ruxsat yo'q!", 403)
    cur.execute("DELETE FROM news_comments WHERE id=%s", (b.comment_id,))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.get("/api/notifications")
def notifications(user_id: int):
    c = db(); cur = c.cursor()
    out = []
    cur.execute("""SELECT l.timestamp ts, u.username, u.fullname, p.content snippet FROM likes l
        JOIN posts p ON p.id=l.post_id JOIN users u ON u.id=l.user_id
        WHERE p.user_id=%s AND l.user_id!=%s AND l.is_like=1 ORDER BY l.id DESC LIMIT 10""",
        (user_id, user_id))
    for r in cur.fetchall(): out.append({"type": "like", **dict(r)})
    
    cur.execute("""SELECT m.timestamp ts, u.username, u.fullname, m.content snippet FROM comments m
        JOIN posts p ON p.id=m.post_id JOIN users u ON u.id=m.user_id
        WHERE p.user_id=%s AND m.user_id!=%s ORDER BY m.id DESC LIMIT 10""",
        (user_id, user_id))
    for r in cur.fetchall(): out.append({"type": "comment", **dict(r)})
    
    cur.execute("""SELECT m.timestamp ts, u.username, u.fullname, m.message snippet FROM chat_messages m
        JOIN users u ON u.id=m.sender_id WHERE m.receiver_id=%s ORDER BY m.id DESC LIMIT 10""",
        (user_id,))
    for r in cur.fetchall(): out.append({"type": "message", **dict(r)})
    
    cur.execute("SELECT timestamp ts, author username, author fullname, title snippet FROM school_news ORDER BY id DESC LIMIT 5")
    for r in cur.fetchall(): out.append({"type": "news", **dict(r)})
    
    cur.close(); c.close()
    out.sort(key=lambda x: str(x["ts"]) if x["ts"] else "", reverse=True)
    return out[:30]

@app.post("/api/admin/news_rights")
def rights(b: RightsReq):
    c = db(); cur = c.cursor()
    boss = urow(c, b.boss_id)
    if not boss or boss["username"] != "boss": cur.close(); c.close(); return err("Faqat @boss!", 403)
    cur.execute("SELECT id,can_post_news FROM users WHERE username=%s", (clean_u(b.target_username),))
    tg = cur.fetchone()
    if not tg: cur.close(); c.close(); return err("Topilmadi!", 404)
    nv = 0 if tg["can_post_news"] == 1 else 1
    cur.execute("UPDATE users SET can_post_news=%s WHERE id=%s", (nv, tg["id"]))
    c.commit(); cur.close(); c.close()
    return {"granted": bool(nv)}

@app.get("/api/ceo")
def ceo():
    c = db(); cur = c.cursor()
    cur.execute("SELECT * FROM ceo_page WHERE id=1")
    r = cur.fetchone()
    cur.close(); c.close()
    return dict(r) if r else {}

@app.post("/api/ceo/update")
def ceo_update(b: CeoUpd):
    c = db(); cur = c.cursor()
    boss = urow(c, b.user_id)
    if not boss or boss["username"] != "boss": cur.close(); c.close(); return err("Faqat @boss!", 403)
    cur.execute("UPDATE ceo_page SET name=%s,title=%s,bio=%s,telegram=%s,instagram=%s,chalker=%s WHERE id=1",
              (b.name.strip(), b.title.strip(), b.bio.strip(), b.telegram.strip(), b.instagram.strip(), b.chalker.strip()))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.get("/api/ceo/list")
def ceo_list():
    c = db(); cur = c.cursor()
    cur.execute("SELECT * FROM ceo_extra ORDER BY id ASC")
    rows = cur.fetchall()
    cur.close(); c.close()
    return [dict(r) for r in rows]

@app.post("/api/ceo/create")
def ceo_create(b: CeoCreate):
    c = db(); cur = c.cursor()
    boss = urow(c, b.user_id)
    if not boss or boss["username"] != "boss": cur.close(); c.close(); return err("Faqat @boss!", 403)
    if not b.name.strip(): cur.close(); c.close(); return err("Ism majburiy!")
    cur.execute("INSERT INTO ceo_extra(name,title,bio,telegram,instagram,chalker) VALUES(%s,%s,%s,%s,%s,%s) RETURNING id",
              (b.name.strip(), b.title.strip(), b.bio.strip(), b.telegram.strip(), b.instagram.strip(), b.chalker.strip()))
    new_id = cur.fetchone()["id"]
    c.commit(); cur.close(); c.close()
    return {"success": True, "id": new_id}

@app.post("/api/ceo/extra/update")
def ceo_extra_update(b: CeoExtraUpd):
    c = db(); cur = c.cursor()
    boss = urow(c, b.user_id)
    if not boss or boss["username"] != "boss": cur.close(); c.close(); return err("Faqat @boss!", 403)
    if not b.name.strip(): cur.close(); c.close(); return err("Ism majburiy!")
    cur.execute("SELECT id FROM ceo_extra WHERE id=%s", (b.ceo_id,))
    if not cur.fetchone():
        cur.close(); c.close(); return err("Topilmadi!", 404)
    cur.execute("UPDATE ceo_extra SET name=%s,title=%s,bio=%s,telegram=%s,instagram=%s,chalker=%s WHERE id=%s",
              (b.name.strip(), b.title.strip(), b.bio.strip(), b.telegram.strip(), b.instagram.strip(), b.chalker.strip(), b.ceo_id))
    c.commit(); cur.close(); c.close()
    return {"success": True}

@app.post("/api/ceo/extra/delete")
def ceo_extra_delete(b: CeoExtraDel):
    c = db(); cur = c.cursor()
    boss = urow(c, b.user_id)
    if not boss or boss["username"] != "boss": cur.close(); c.close(); return err("Faqat @boss!", 403)
    cur.execute("DELETE FROM ceo_extra WHERE id=%s", (b.ceo_id,))
    c.commit(); cur.close(); c.close()
    return {"success": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
