import sqlite3, os
from typing import Optional
from fastapi import FastAPI, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

DB = "chalker_v4.db"
INDEX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "index.html")
app = FastAPI(title="Chalker")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def db():
    c = sqlite3.connect(DB); c.row_factory = sqlite3.Row; return c

def init():
    c = db()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL,
      fullname TEXT, school_class TEXT, school_name TEXT, country TEXT, region TEXT, district TEXT,
      role TEXT DEFAULT 'student', birth_date TEXT, hide_birth_date INTEGER DEFAULT 0, bio TEXT,
      heart_status TEXT DEFAULT 'Available', avatar_base64 TEXT, can_post_news INTEGER DEFAULT 0, password TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS posts(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
      content TEXT, media_base64 TEXT, media_type TEXT, timestamp TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS comments(id INTEGER PRIMARY KEY AUTOINCREMENT, post_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL, parent_id INTEGER, content TEXT NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS likes(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
      post_id INTEGER NOT NULL, is_like INTEGER NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE UNIQUE INDEX IF NOT EXISTS iup ON likes(user_id, post_id);
    CREATE TABLE IF NOT EXISTS chat_messages(id INTEGER PRIMARY KEY AUTOINCREMENT, sender_id INTEGER NOT NULL,
      receiver_id INTEGER NOT NULL, message TEXT NOT NULL, image_base64 TEXT, timestamp TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS follows(follower_id INTEGER NOT NULL, following_id INTEGER NOT NULL,
      PRIMARY KEY(follower_id, following_id));
    CREATE TABLE IF NOT EXISTS school_news(id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL,
      author TEXT, timestamp TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS news_likes(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
      news_id INTEGER NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE UNIQUE INDEX IF NOT EXISTS iun ON news_likes(user_id, news_id);
    CREATE TABLE IF NOT EXISTS news_comments(id INTEGER PRIMARY KEY AUTOINCREMENT, news_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL, content TEXT NOT NULL, timestamp TEXT DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS ceo_page(id INTEGER PRIMARY KEY, name TEXT, title TEXT, bio TEXT,
      telegram TEXT, instagram TEXT, chalker TEXT);
    CREATE TABLE IF NOT EXISTS ceo_extra(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, title TEXT, bio TEXT,
      telegram TEXT, instagram TEXT, chalker TEXT, timestamp TEXT DEFAULT CURRENT_TIMESTAMP);
    """)
    if not c.execute("SELECT id FROM ceo_page WHERE id=1").fetchone():
        c.execute("INSERT INTO ceo_page VALUES(1,?,?,?,?,?,?)",
                  ("Ilyosbek Siddiqjonov", "CEO Founder",
                   "Chalker — o'quvchilar va o'qituvchilar uchun zamonaviy maktab ijtimoiy tarmog'i.",
                   "@ilyos6ee", "@ilyos6ee", "@boss"))
    existing_cols = [r["name"] for r in c.execute("PRAGMA table_info(chat_messages)").fetchall()]
    if "image_base64" not in existing_cols:
        c.execute("ALTER TABLE chat_messages ADD COLUMN image_base64 TEXT")
    if "is_read" not in existing_cols:
        c.execute("ALTER TABLE chat_messages ADD COLUMN is_read INTEGER DEFAULT 0")
    if "edited" not in existing_cols:
        c.execute("ALTER TABLE chat_messages ADD COLUMN edited INTEGER DEFAULT 0")
    c.commit(); c.close()

init()

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
    if len(password) < 4: return err("Parol kamida 4 belgi!")
    c = db()
    if c.execute("SELECT id FROM users WHERE username=?", (u,)).fetchone():
        c.close(); return err("Bu username band!")
    cur = c.execute("INSERT INTO users(username,fullname,password) VALUES(?,?,?)", (u, fullname.strip(), password))
    c.commit(); r = urow(c, cur.lastrowid); c.close()
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
            heart_status: str = Form("Available"), avatar_base64: str = Form("")):
    c = db()
    c.execute("""UPDATE users SET fullname=?,school_class=?,school_name=?,country=?,region=?,district=?,
              role=?,bio=?,birth_date=?,hide_birth_date=?,heart_status=? WHERE id=?""",
              (fullname.strip(), school_class, school_name, country, region, district, role, bio.strip(),
               birth_date, int(hide_birth_date), heart_status, user_id))
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
    d["followers"] = c.execute("SELECT COUNT(*) FROM follows WHERE following_id=?", (r["id"],)).fetchone()[0]
    d["following"] = c.execute("SELECT COUNT(*) FROM follows WHERE follower_id=?", (r["id"],)).fetchone()[0]
    d["is_following"] = bool(viewer_id and c.execute(
        "SELECT 1 FROM follows WHERE follower_id=? AND following_id=?", (viewer_id, r["id"])).fetchone())
    if int(d.get("hide_birth_date") or 0) == 1 and (viewer_id is None or int(viewer_id) != r["id"]):
        d["birth_date"] = None
    c.close(); return d

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
def posts(user_id: Optional[int] = None):
    v = user_id if user_id is not None else -1
    c = db()
    rows = c.execute("""SELECT p.id,p.user_id,p.content,p.media_base64,p.media_type,p.timestamp,
        u.username,u.fullname,u.avatar_base64,u.can_post_news,
        (SELECT COUNT(*) FROM likes l WHERE l.post_id=p.id AND l.is_like=1) likes_count,
        (SELECT COUNT(*) FROM comments cm WHERE cm.post_id=p.id) comments_count,
        (SELECT l.is_like FROM likes l WHERE l.post_id=p.id AND l.user_id=?) my_status
        FROM posts p JOIN users u ON u.id=p.user_id ORDER BY p.id DESC""", (v,)).fetchall()
    c.close(); return [dict(r) for r in rows]

@app.post("/api/posts/like")
def post_like(b: LikeReq):
    c = db()
    r = c.execute("SELECT is_like FROM likes WHERE user_id=? AND post_id=?", (b.user_id, b.post_id)).fetchone()
    if r and r["is_like"] == b.is_like:
        c.execute("DELETE FROM likes WHERE user_id=? AND post_id=?", (b.user_id, b.post_id)); liked = False
    else:
        c.execute("INSERT OR REPLACE INTO likes(user_id,post_id,is_like) VALUES(?,?,?)",
                  (b.user_id, b.post_id, b.is_like)); liked = True
    c.commit(); c.close(); return {"liked": liked}

@app.post("/api/comments/create")
def comment_create(b: CommentCreate):
    if not b.content.strip(): return err("Komment bo'sh!")
    c = db()
    c.execute("INSERT INTO comments(post_id,user_id,parent_id,content) VALUES(?,?,?,?)",
              (b.post_id, b.user_id, b.parent_id, b.content.strip()))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/comments")
def comments(post_id: int):
    c = db()
    rows = c.execute("""SELECT c.id,c.parent_id,c.content,c.timestamp,u.username,u.fullname,
        u.avatar_base64,u.can_post_news FROM comments c JOIN users u ON u.id=c.user_id
        WHERE c.post_id=? ORDER BY c.id ASC""", (post_id,)).fetchall()
    c.close(); return [dict(r) for r in rows]

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

@app.post("/api/chat/send")
def chat_send(b: ChatSend):
    if not b.message.strip() and not b.image_base64: return err("Xabar bo'sh!")
    c = db()
    r = c.execute("SELECT id FROM users WHERE username=?", (clean_u(b.receiver_username),)).fetchone()
    if not r: c.close(); return err("Qabul qiluvchi topilmadi!", 404)
    c.execute("INSERT INTO chat_messages(sender_id,receiver_id,message,image_base64) VALUES(?,?,?,?)",
              (b.sender_id, r["id"], b.message.strip(), b.image_base64))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/chat/edit")
def chat_edit(b: ChatEdit):
    if not b.message.strip(): return err("Xabar bo'sh!")
    c = db()
    r = c.execute("SELECT sender_id FROM chat_messages WHERE id=?", (b.message_id,)).fetchone()
    if not r: c.close(); return err("Xabar topilmadi!", 404)
    if r["sender_id"] != b.user_id: c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("UPDATE chat_messages SET message=?,edited=1 WHERE id=?", (b.message.strip(), b.message_id))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/chat/delete")
def chat_delete(b: ChatDel):
    c = db()
    r = c.execute("SELECT sender_id FROM chat_messages WHERE id=?", (b.message_id,)).fetchone()
    if not r: c.close(); return err("Xabar topilmadi!", 404)
    if r["sender_id"] != b.user_id: c.close(); return err("Ruxsat yo'q!", 403)
    c.execute("DELETE FROM chat_messages WHERE id=?", (b.message_id,))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/chat/history")
def chat_history(user_id: int, partner_username: str):
    c = db()
    p = c.execute("SELECT id,username,fullname,avatar_base64,can_post_news FROM users WHERE username=?",
                  (clean_u(partner_username),)).fetchone()
    if not p: c.close(); return err("Foydalanuvchi topilmadi!", 404)
    c.execute("UPDATE chat_messages SET is_read=1 WHERE sender_id=? AND receiver_id=? AND is_read=0",
              (p["id"], user_id))
    c.commit()
    rows = c.execute("""SELECT * FROM chat_messages WHERE (sender_id=? AND receiver_id=?)
        OR (sender_id=? AND receiver_id=?) ORDER BY id ASC""",
        (user_id, p["id"], p["id"], user_id)).fetchall()
    c.close(); return {"partner": dict(p), "messages": [dict(r) for r in rows]}

@app.get("/api/chat/active_users")
def chat_users(user_id: int):
    c = db()
    rows = c.execute("""SELECT u.id,u.username,u.fullname,u.avatar_base64,u.can_post_news,
        (SELECT message FROM chat_messages m WHERE (m.sender_id=u.id AND m.receiver_id=?)
         OR (m.sender_id=? AND m.receiver_id=u.id) ORDER BY m.id DESC LIMIT 1) last_message,
        (SELECT COUNT(*) FROM chat_messages m WHERE m.sender_id=u.id AND m.receiver_id=? AND m.is_read=0) unread_count
        FROM users u WHERE u.id IN (SELECT sender_id FROM chat_messages WHERE receiver_id=?
        UNION SELECT receiver_id FROM chat_messages WHERE sender_id=?) AND u.id!=?""",
        (user_id, user_id, user_id, user_id, user_id, user_id)).fetchall()
    c.close(); return [dict(r) for r in rows]

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
def news_comments(news_id: int):
    c = db()
    rows = c.execute("""SELECT m.id,m.content,m.timestamp,u.username,u.fullname,u.avatar_base64,u.can_post_news
        FROM news_comments m JOIN users u ON u.id=m.user_id WHERE m.news_id=? ORDER BY m.id ASC""",
        (news_id,)).fetchall()
    c.close(); return [dict(r) for r in rows]

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
    for r in c.execute("""SELECT m.timestamp ts, u.username, u.fullname, m.message snippet FROM chat_messages m
        JOIN users u ON u.id=m.sender_id WHERE m.receiver_id=? ORDER BY m.id DESC LIMIT 10""",
        (user_id,)).fetchall():
        out.append({"type": "message", **dict(r)})
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

@app.get("/api/ceo")
def ceo():
    c = db(); r = c.execute("SELECT * FROM ceo_page WHERE id=1").fetchone(); c.close(); return dict(r)

@app.post("/api/ceo/update")
def ceo_update(b: CeoUpd):
    c = db(); boss = urow(c, b.user_id)
    if not boss or boss["username"] != "boss": c.close(); return err("Faqat @boss!", 403)
    c.execute("UPDATE ceo_page SET name=?,title=?,bio=?,telegram=?,instagram=?,chalker=? WHERE id=1",
              (b.name.strip(), b.title.strip(), b.bio.strip(), b.telegram.strip(), b.instagram.strip(), b.chalker.strip()))
    c.commit(); c.close(); return {"success": True}

@app.get("/api/ceo/list")
def ceo_list():
    c = db()
    rows = c.execute("SELECT * FROM ceo_extra ORDER BY id ASC").fetchall()
    c.close(); return [dict(r) for r in rows]

@app.post("/api/ceo/create")
def ceo_create(b: CeoCreate):
    c = db(); boss = urow(c, b.user_id)
    if not boss or boss["username"] != "boss": c.close(); return err("Faqat @boss!", 403)
    if not b.name.strip(): c.close(); return err("Ism majburiy!")
    cur = c.execute("INSERT INTO ceo_extra(name,title,bio,telegram,instagram,chalker) VALUES(?,?,?,?,?,?)",
              (b.name.strip(), b.title.strip(), b.bio.strip(), b.telegram.strip(), b.instagram.strip(), b.chalker.strip()))
    c.commit(); c.close(); return {"success": True, "id": cur.lastrowid}

@app.post("/api/ceo/extra/update")
def ceo_extra_update(b: CeoExtraUpd):
    c = db(); boss = urow(c, b.user_id)
    if not boss or boss["username"] != "boss": c.close(); return err("Faqat @boss!", 403)
    if not b.name.strip(): c.close(); return err("Ism majburiy!")
    if not c.execute("SELECT id FROM ceo_extra WHERE id=?", (b.ceo_id,)).fetchone():
        c.close(); return err("Topilmadi!", 404)
    c.execute("UPDATE ceo_extra SET name=?,title=?,bio=?,telegram=?,instagram=?,chalker=? WHERE id=?",
              (b.name.strip(), b.title.strip(), b.bio.strip(), b.telegram.strip(), b.instagram.strip(), b.chalker.strip(), b.ceo_id))
    c.commit(); c.close(); return {"success": True}

@app.post("/api/ceo/extra/delete")
def ceo_extra_delete(b: CeoExtraDel):
    c = db(); boss = urow(c, b.user_id)
    if not boss or boss["username"] != "boss": c.close(); return err("Faqat @boss!", 403)
    c.execute("DELETE FROM ceo_extra WHERE id=?", (b.ceo_id,))
    c.commit(); c.close(); return {"success": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
