# books_api.py
# FastAPI server for Books Log with Google Drive-backed SQLite (sync ON EVERY change)
# Usage:
#  - set Railway environment variables (see README below)
#  - uvicorn books_api:app --host 0.0.0.0 --port $PORT

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
import base64
import json

import os
import io
import sqlite3
import csv
import traceback
from typing import List, Optional

from fastapi import UploadFile, File
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

# Google Drive client libs
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

# ---------------------------
# Configuration via env vars
# ---------------------------
API_KEY = os.environ.get("BOOKS_API_KEY")
if not API_KEY:
    raise RuntimeError("BOOKS_API_KEY environment variable is NOT set! Set it in Railway Variables.")

# file names inside the container (will be transient; we sync to Drive)
DB_FILE = os.environ.get("BOOKS_DB_FILE", "books.db")
BACKUP_CSV = os.environ.get("BOOKS_BACKUP_CSV", "books_backup.csv")
BOOKS_OWNER_EMAIL = os.environ.get("BOOKS_OWNER_EMAIL")  # your personal Google email
EBOOKS_FOLDER_ID = os.environ.get("EBOOKS_FOLDER_ID")
OAUTH_CREDENTIALS_B64 = os.environ.get("OAUTH_CREDENTIALS_B64")
OAUTH_TOKEN_B64 = os.environ.get("OAUTH_TOKEN_B64")

# Google Drive related env vars
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")  # full JSON string
GOOGLE_DRIVE_FILE_ID = os.environ.get("GOOGLE_DRIVE_FILE_ID")  # file id of books.db in your Drive

# Validate Drive envs (we allow missing if user intends not to use Drive, but for this setup they must exist)
if not GOOGLE_SERVICE_ACCOUNT_JSON or not GOOGLE_DRIVE_FILE_ID:
    raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON and GOOGLE_DRIVE_FILE_ID must be set in environment variables.")

# Parse service account JSON
try:
    sa_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
except Exception as e:
    raise RuntimeError("Failed to parse GOOGLE_SERVICE_ACCOUNT_JSON. Make sure you pasted the JSON content exactly.") from e


SCOPES = ["https://www.googleapis.com/auth/drive.file"]
# Drive scopes (service account needs broader scope for DB operations)
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]

# Create credentials and service object (will be used for upload/download of DB)
try:
    credentials = service_account.Credentials.from_service_account_info(sa_info, scopes=DRIVE_SCOPES)
    drive_service = build("drive", "v3", credentials=credentials, cache_discovery=False)
except Exception as e:
    raise RuntimeError("Failed to initialize Google Drive client. Check service account JSON and network.") from e

# ---------------------------
# FastAPI app
# ---------------------------
app = FastAPI(title="Books Log API (Drive-backed)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for production if you want
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------
# Models
# ---------------------------
class BookIn(BaseModel):
    title: str
    author: str
    status: Optional[str] = ""
    rating: Optional[str] = ""
    notes: Optional[str] = ""
    file_path: Optional[str] = ""

class BookOut(BookIn):
    id: int

# ---------------------------
# Auth helper
# ---------------------------
def check_key(x_api_key: Optional[str]):
    if x_api_key is None:
        raise HTTPException(status_code=401, detail="Missing API key")
    # strip to avoid hidden-space mismatches
    if x_api_key.strip() != API_KEY.strip():
        raise HTTPException(status_code=401, detail="Invalid API key")

# ---------------------------
# Google Drive helpers (service account for DB)
# ---------------------------
def download_db_from_drive():
    """
    Download the Google Drive file (GOOGLE_DRIVE_FILE_ID) and save it to DB_FILE.
    If the file does not exist on Drive (404), create an empty DB locally and upload it.
    """
    try:
        request = drive_service.files().get_media(fileId=GOOGLE_DRIVE_FILE_ID)
        fh = io.FileIO(DB_FILE, mode="wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.close()
        print("Downloaded DB from Drive to", DB_FILE)
        return True
    except HttpError as e:
        # 404 means the file id might be wrong or not accessible
        code = None
        try:
            code = e.resp.status
        except Exception:
            pass
        print("Google Drive download error:", e)
        if code == 404:
            # Create empty DB and upload it
            print("Drive file not found (404). Creating a new empty DB and uploading it.")
            init_db_local()
            upload_db_to_drive()
            return True
        raise

def upload_db_to_drive():
    """
    Upload the local DB_FILE to the existing Drive file ID by using files().update.
    If update fails because file not found, try create (rare because we were given an ID).
    """
    try:
        media = MediaFileUpload(DB_FILE, mimetype="application/octet-stream", resumable=True)
        updated = drive_service.files().update(fileId=GOOGLE_DRIVE_FILE_ID, media_body=media).execute()
        print("Uploaded DB to Drive (updated):", updated.get("id"))
        return True
    except HttpError as e:
        print("Drive upload (update) error:", e)
        # if file not found or permission, try create (useful if user gave folder id accidentally)
        try:
            # attempt to create a new file (this will not replace the original file id)
            file_metadata = {"name": os.path.basename(DB_FILE)}
            media = MediaFileUpload(DB_FILE, mimetype="application/octet-stream", resumable=True)
            created = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
            print("Uploaded DB to Drive (created new file):", created.get("id"))
            return True
        except Exception as ee:
            print("Failed to upload DB to Drive:", ee)
            raise

# ---------------------------
# SQLite helpers
# ---------------------------
def init_db_local():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY,
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

def fetch_all_books_local():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT id, title, author, status, rating, notes, file_path FROM books ORDER BY id")
    rows = cur.fetchall()
    conn.close()
    return rows

def insert_book_local_with_id(book_id, title, author, status, rating, notes, file_path):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("INSERT INTO books (id, title, author, status, rating, notes, file_path) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (book_id, title, author, status, rating, notes, file_path))
    conn.commit()
    conn.close()

def get_next_id_local():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT MAX(id) FROM books")
    row = cur.fetchone()
    conn.close()
    maxid = row[0] if row and row[0] is not None else 0
    return maxid + 1

# ---------------------------
# Startup: download DB (or create & upload)
# ---------------------------
@app.on_event("startup")
def startup_event():
    # Attempt to download DB from Drive; create & upload if missing
    try:
        download_db_from_drive()
    except Exception as e:
        print("Failed to download DB from Drive on startup:", e)
        traceback.print_exc()
        # As a fallback, ensure a local DB exists
        init_db_local()
    # Ensure local DB has table
    init_db_local()

# ---------------------------
# API endpoints
# ---------------------------
@app.get("/books", response_model=List[BookOut])
def get_books(x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)
    rows = fetch_all_books_local()
    return [BookOut(id=r[0], title=r[1], author=r[2], status=r[3], rating=r[4], notes=r[5], file_path=r[6]) for r in rows]

@app.post("/books", response_model=BookOut)
def add_book(b: BookIn, x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)
    # insert with next id
    new_id = get_next_id_local()
    insert_book_local_with_id(new_id, b.title, b.author, b.status, b.rating, b.notes, b.file_path)
    # upload DB to Drive (sync)
    try:
        upload_db_to_drive()
    except Exception as e:
        print("Upload after add failed:", e)
        # continue — server still returns success; client can retry backup
    return BookOut(id=new_id, **b.dict())

@app.put("/books/{book_id}", response_model=BookOut)
def update_book(book_id: int, b: BookIn, x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT id FROM books WHERE id=?", (book_id,))
    if cur.fetchone() is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Book not found")
    cur.execute("""UPDATE books SET title=?, author=?, status=?, rating=?, notes=?, file_path=? WHERE id=?""",
                (b.title, b.author, b.status, b.rating, b.notes, b.file_path, book_id))
    conn.commit()
    conn.close()
    # upload DB to Drive
    try:
        upload_db_to_drive()
    except Exception as e:
        print("Upload after update failed:", e)
    return BookOut(id=book_id, **b.dict())

def get_oauth_drive_service():
    """Return Google Drive client authenticated as YOUR Gmail using OAuth."""
    if not OAUTH_CREDENTIALS_B64:
        raise HTTPException(500, "OAuth credentials missing")

    # Decode credentials.json
    cred_data = json.loads(base64.b64decode(OAUTH_CREDENTIALS_B64))

    # Load token if it exists
    token_data = None
    if OAUTH_TOKEN_B64:
        token_data = json.loads(base64.b64decode(OAUTH_TOKEN_B64))

    creds = None
    if token_data:
        creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    else:
        raise HTTPException(500, "OAuth token missing. You must authorize first.")

    # Refresh token if needed
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return build("drive", "v3", credentials=creds)


@app.delete("/books/{book_id}")
def delete_book(book_id: int, x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # 1. Delete the book
    cur.execute("DELETE FROM books WHERE id=?", (book_id,))
    conn.commit()

    # 2. Fetch remaining books ordered by current ID (without ids)
    cur.execute("SELECT title, author, status, rating, notes, file_path FROM books ORDER BY id")
    rows = cur.fetchall()

    # 3. Clear the table
    cur.execute("DELETE FROM books")
    conn.commit()

    # 4. Reinsert rows with NEW continuous IDs starting from 1
    new_id = 1
    for r in rows:
        cur.execute(
            "INSERT INTO books (id, title, author, status, rating, notes, file_path) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (new_id, r[0], r[1], r[2], r[3], r[4], r[5])
        )
        new_id += 1

    conn.commit()
    conn.close()

    # upload DB to Drive
    try:
        upload_db_to_drive()
    except Exception as e:
        print("Upload after delete failed:", e)

    return {"detail": "deleted_and_renumbered"}

@app.post("/save_all")
def save_all(payload: List[BookOut], x_api_key: Optional[str] = Header(None)):
    """
    Replace entire DB with the provided list (used by client save_all).
    Payload: list of BookOut-like dicts (with id, title, author, ...)
    We'll rewrite the table and renumber ids sequentially based on provided order.
    """
    check_key(x_api_key)
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute("DELETE FROM books")
        new_id = 1
        for item in payload:
            cur.execute(
                "INSERT INTO books (id, title, author, status, rating, notes, file_path) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (new_id, item.get("title", ""), item.get("author", ""), item.get("status", ""), item.get("rating", ""), item.get("notes", ""), item.get("file_path", "")) 
            )
            new_id += 1
        conn.commit()
        conn.close()
        # upload DB to Drive
        try:
            upload_db_to_drive()
        except Exception as e:
            print("Upload after save_all failed:", e)
        return {"detail": "saved"}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="save_all failed")

import secrets
import urllib.parse

@app.get("/oauth_start")
def oauth_start():
    """Start OAuth login with a fixed redirect_uri."""
    if not OAUTH_CREDENTIALS_B64:
        raise HTTPException(500, "OAuth credentials missing")

    cred_data = json.loads(base64.b64decode(OAUTH_CREDENTIALS_B64))

    redirect_uri = cred_data["installed"]["redirect_uris"][0]  # should be http://localhost

    flow = InstalledAppFlow.from_client_config(
        cred_data,
        SCOPES
    )


    # IMPORTANT: Only set redirect_uri in THIS place
    flow.redirect_uri = redirect_uri

    auth_url, _ = flow.authorization_url(
        prompt="consent",
        access_type="offline",
        include_granted_scopes="true"
    )

    return {
        "auth_url": auth_url,
        "redirect_to_use": redirect_uri
    }


@app.get("/oauth_finish")
def oauth_finish(code: str):
    """Finish OAuth and return token to store in Railway."""
    if not OAUTH_CREDENTIALS_B64:
        raise HTTPException(500, "OAuth credentials missing")

    cred_data = json.loads(base64.b64decode(OAUTH_CREDENTIALS_B64))

    redirect_uri = cred_data["installed"]["redirect_uris"][0]

    flow = InstalledAppFlow.from_client_config(
        cred_data,
        SCOPES
    )


    # AGAIN — only this line should set redirect_uri
    flow.redirect_uri = redirect_uri

    # FIXED: No redirect_uri parameter here
    flow.fetch_token(code=code)

    token_json = json.dumps({
        "token": flow.credentials.token,
        "refresh_token": flow.credentials.refresh_token,
        "token_uri": flow.credentials.token_uri,
        "client_id": flow.credentials.client_id,
        "client_secret": flow.credentials.client_secret,
        "scopes": flow.credentials.scopes
    })

    encoded = base64.b64encode(token_json.encode()).decode()

    return {
        "message": "Paste this into Railway as OAUTH_TOKEN_B64",
        "base64_token": encoded
    }


@app.post("/upload_ebook")
async def upload_ebook(
    file: UploadFile = File(...),
    x_api_key: Optional[str] = Header(None)
):
    """
    Upload an eBook file into the eBooks folder in Google Drive using OAuth (your Gmail).
    Returns public links used for file_path.
    """
    check_key(x_api_key)

    if not EBOOKS_FOLDER_ID:
        raise HTTPException(
            status_code=500,
            detail="EBOOKS_FOLDER_ID not configured on the server."
        )

    # 1. Save incoming file to temporary storage
    temp_dir = "/tmp"
    os.makedirs(temp_dir, exist_ok=True)
    temp_path = os.path.join(temp_dir, file.filename)

    try:
        contents = await file.read()
        with open(temp_path, "wb") as f:
            f.write(contents)

        # 2. Build OAuth drive service (acts as your Gmail)
        try:
            drive = get_oauth_drive_service()
        except HTTPException as he:
            # pass through helpful error
            raise he
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Failed to initialize OAuth Drive service: {str(e)}")

        # 3. Metadata for the Drive file (use the eBooks folder)
        file_metadata = {
            "name": file.filename,
            "parents": [EBOOKS_FOLDER_ID]
        }

        media = MediaFileUpload(temp_path, resumable=True)

        # 4. Upload file to Drive inside eBooks folder using OAuth drive client
        try:
            created = drive.files().create(
                body=file_metadata,
                media_body=media,
                fields="id, webViewLink, webContentLink"
            ).execute()
        except HttpError as he:
            traceback.print_exc()
            # surface a clearer message
            raise HTTPException(status_code=500, detail=f"Upload ebook failed (Drive API): {str(he)}")

        ebook_id = created.get("id")
        view_url = created.get("webViewLink")
        download_url = created.get("webContentLink")

        # 5. Share with main Google account (viewer) using OAuth drive client
        if BOOKS_OWNER_EMAIL:
            try:
                permission_body = {
                    "type": "user",
                    "role": "reader",
                    "emailAddress": BOOKS_OWNER_EMAIL
                }
                drive.permissions().create(
                    fileId=ebook_id,
                    body=permission_body,
                    sendNotificationEmail=False
                ).execute()
            except Exception as e:
                # non-fatal: print and continue
                print("Failed to set viewer permission for owner email (non-fatal):", e)

        # 6. Cleanup
        try:
            os.remove(temp_path)
        except:
            pass

        # 7. Return URLs for mobile app / PC
        return {
            "id": ebook_id,
            "webViewLink": view_url,
            "webContentLink": download_url
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Upload ebook failed: {str(e)}"
        )


@app.get("/backup")
def backup(x_api_key: Optional[str] = Header(None)):
    check_key(x_api_key)

    try:
        # -----------------------------
        # 1. Read DB into CSV
        # -----------------------------
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute("SELECT id, title, author, status, rating, notes, file_path FROM books")
        rows = cur.fetchall()
        col_names = [column[0] for column in cur.description]
        conn.close()

        # Write CSV locally
        with open(BACKUP_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(col_names)
            writer.writerows(rows)

        # -----------------------------
        # 2. Get parent folder of books.db on Google Drive
        # -----------------------------
        file_info = drive_service.files().get(fileId=GOOGLE_DRIVE_FILE_ID, fields="parents").execute()
        parents = file_info.get("parents", None)

        if not parents:
            raise HTTPException(status_code=500, detail="Unable to retrieve parent folder of books.db from Drive.")

        parent_folder_id = parents[0]

        # -----------------------------
        # 3. Upload backup CSV to SAME folder
        # -----------------------------
        file_metadata = {
            "name": BACKUP_CSV,      # same file name
            "parents": [parent_folder_id]
        }

        media = MediaFileUpload(BACKUP_CSV, mimetype="text/csv", resumable=True)

        # Search if a backup file already exists to update it instead of making duplicates
        query = f"name = '{BACKUP_CSV}' and '{parent_folder_id}' in parents"
        search = drive_service.files().list(q=query, fields="files(id)").execute()
        existing_files = search.get('files', [])

        if existing_files:
            backup_id = existing_files[0]['id']
            drive_service.files().update(fileId=backup_id, media_body=media).execute()
            action = "updated"
        else:
            created = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
            action = "created"

        return {"detail": f"backup_{action}_in_drive"}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Backup failed: {str(e)}")
