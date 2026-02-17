import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Literal

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

DB_PATH = os.getenv("TICKET_DB_PATH", "/data/tickets.db")

templates = Jinja2Templates(directory="templates")
app = FastAPI(title="Ticket UI (Jira-like Kanban)")

Status = Literal["TODO", "IN_PROGRESS", "DONE"]
Severity = Literal["P0", "P1", "P2", "P3"]

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tickets (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      key TEXT NOT NULL UNIQUE,
      title TEXT NOT NULL,
      description TEXT NOT NULL,
      service TEXT NOT NULL,
      env TEXT NOT NULL,
      severity TEXT NOT NULL,
      status TEXT NOT NULL,
      event_id TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS comments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ticket_id INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      author TEXT NOT NULL,
      body TEXT NOT NULL,
      FOREIGN KEY(ticket_id) REFERENCES tickets(id)
    )
    """)
    conn.commit()
    conn.close()

@app.on_event("startup")
def _startup():
    init_db()

def next_key(conn) -> str:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM tickets")
    c = cur.fetchone()["c"]
    return f"OPS-{c + 1}"

class TicketCreate(BaseModel):
    title: str
    description: str
    service: str
    env: str
    severity: Severity = "P2"
    event_id: Optional[str] = None

class TicketPatch(BaseModel):
    status: Optional[Status] = None
    severity: Optional[Severity] = None
    title: Optional[str] = None

class CommentCreate(BaseModel):
    author: str = "n8n"
    body: str

@app.get("/", response_class=HTMLResponse)
def board(request: Request):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tickets ORDER BY updated_at DESC")
    tickets = [dict(r) for r in cur.fetchall()]
    conn.close()

    cols = {"TODO": [], "IN_PROGRESS": [], "DONE": []}
    for t in tickets:
        cols.setdefault(t["status"], []).append(t)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "cols": cols
    })

@app.get("/api/board")
def api_board():
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tickets ORDER BY updated_at DESC")
    tickets = [dict(r) for r in cur.fetchall()]
    conn.close()
    cols = {"TODO": [], "IN_PROGRESS": [], "DONE": []}
    for t in tickets:
        cols.setdefault(t["status"], []).append(t)
    return cols

@app.post("/api/tickets")
def api_create_ticket(payload: TicketCreate):
    conn = db()
    cur = conn.cursor()

    key = next_key(conn)
    ts = now_iso()
    cur.execute("""
      INSERT INTO tickets(key,title,description,service,env,severity,status,event_id,created_at,updated_at)
      VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (key, payload.title, payload.description, payload.service, payload.env,
          payload.severity, "TODO", payload.event_id, ts, ts))
    ticket_id = cur.lastrowid
    conn.commit()

    cur.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,))
    ticket = dict(cur.fetchone())
    conn.close()
    return ticket

@app.patch("/api/tickets/{ticket_id}")
def api_patch_ticket(ticket_id: int, patch: TicketPatch):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Ticket not found")

    fields = []
    vals = []
    if patch.status is not None:
        fields.append("status=?")
        vals.append(patch.status)
    if patch.severity is not None:
        fields.append("severity=?")
        vals.append(patch.severity)
    if patch.title is not None:
        fields.append("title=?")
        vals.append(patch.title)

    if not fields:
        conn.close()
        return dict(row)

    fields.append("updated_at=?")
    vals.append(now_iso())
    vals.append(ticket_id)

    cur.execute(f"UPDATE tickets SET {', '.join(fields)} WHERE id=?", tuple(vals))
    conn.commit()

    cur.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,))
    ticket = dict(cur.fetchone())
    conn.close()
    return ticket

@app.get("/api/tickets/{ticket_id}")
def api_get_ticket(ticket_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Ticket not found")

    cur.execute("SELECT * FROM comments WHERE ticket_id=? ORDER BY created_at ASC", (ticket_id,))
    comments = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"ticket": dict(row), "comments": comments}

@app.post("/api/tickets/{ticket_id}/comment")
def api_add_comment(ticket_id: int, payload: CommentCreate):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM tickets WHERE id=?", (ticket_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Ticket not found")

    ts = now_iso()
    cur.execute("""
      INSERT INTO comments(ticket_id, created_at, author, body)
      VALUES (?,?,?,?)
    """, (ticket_id, ts, payload.author, payload.body))

    cur.execute("UPDATE tickets SET updated_at=? WHERE id=?", (ts, ticket_id))
    conn.commit()
    conn.close()
    return {"ok": True}
