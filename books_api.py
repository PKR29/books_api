# books_api.py
# FastAPI server for Books Log (master DB)
# Usage:
#   pip install fastapi uvicorn
#   export BOOKS_API_KEY="my_secret_key"
#   uvicorn books_api:app --host 0.0.0.0 --port 8000

import sqlite3
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import os
import csv

API_KEY = os.environ.get("BOOKS_API_KEY", "my_secret_key")
DB_FILE = os.environ.get("BOOKS_DB_FILE", "books.db")
BACKUP_CSV = os.environ.get("BOOKS_BACKUP_CSV", "books_backup.csv")

app = FastAPI(title="Books Log API (FastAPI)")

# For initial testing you can allow all origins; tighten in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class BookIn(BaseModel):
    title: str
    author: str
    status: Optional[str] = ""
    rating: Optional[str] = ""
    notes: Optional[str] = ""
    file_path: Optional[str] = ""

class BookOut(BookIn):
    id: int

def check_key(x_api_key: Optional[str]):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

def init_db_if_needed():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            author TEXT,
            status TEXT,
            rating TEXT,
            notes TEXT,
            file_path TEXT
        )
    """)
    conn.commit()
    conn.close()

@app.on_event("startup")
def startup():
    init_db_if_needed()

@app.get("/books", response_model=List[BookOut])
def get_books(x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT id, title, author, status, rating, notes, file_path FROM books")
    rows = cur.fetchall()
    conn.close()
    return [BookOut(id=r[0], title=r[1], author=r[2], status=r[3], rating=r[4], notes=r[5], file_path=r[6]) for r in rows]

@app.post("/books", response_model=BookOut)
def add_book(b: BookIn, x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("INSERT INTO books (title, author, status, rating, notes, file_path) VALUES (?, ?, ?, ?, ?, ?)",
                (b.title, b.author, b.status, b.rating, b.notes, b.file_path))
    conn.commit()
    book_id = cur.lastrowid
    conn.close()
    return BookOut(id=book_id, **b.dict())

@app.put("/books/{book_id}", response_model=BookOut)
def update_book(book_id: int, b: BookIn, x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT id FROM books WHERE id=?", (book_id,))
    if cur.fetchone() is None:
        conn.close()
        raise HTTPException(404, "Book not found")
    cur.execute("""UPDATE books SET title=?, author=?, status=?, rating=?, notes=?, file_path=? WHERE id=?""",
                (b.title, b.author, b.status, b.rating, b.notes, b.file_path, book_id))
    conn.commit()
    conn.close()
    return BookOut(id=book_id, **b.dict())

@app.delete("/books/{book_id}")
def delete_book(book_id: int, x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("DELETE FROM books WHERE id=?", (book_id,))
    conn.commit()
    conn.close()
    return {"detail": "deleted"}

@app.get("/backup")
def backup(x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT id, title, author, status, rating, notes, file_path FROM books")
    rows = cur.fetchall()
    header
