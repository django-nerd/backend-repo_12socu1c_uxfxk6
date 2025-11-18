import os
from typing import List, Optional, Set, Dict, Any
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from database import db, create_document

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------
# Utility helpers
# -----------------------------

def same_origin(base: str, target: str) -> bool:
    try:
        b = urlparse(base)
        t = urlparse(target)
        return (t.scheme in ("http", "https")) and (t.netloc == b.netloc)
    except Exception:
        return False


def clean_url(url: str) -> str:
    # Normalize URL (drop fragments)
    parsed = urlparse(url)
    return parsed._replace(fragment="").geturl()


def extract_tables(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    tables = []
    for table in soup.find_all("table"):
        headers = []
        # Collect header cells
        thead = table.find("thead")
        if thead:
            header_row = thead.find("tr")
            if header_row:
                headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                headers = [th.get_text(strip=True) for th in first_tr.find_all(["th", "td"])]

        rows = []
        tbody = table.find("tbody") or table
        for tr in tbody.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if cells:
                # Skip duplicate header row if same as headers
                if headers and cells == headers:
                    continue
                rows.append(cells)

        if headers or rows:
            tables.append({"headers": headers, "rows": rows})
    return tables


def fetch_page(url: str) -> Dict[str, Any]:
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=f"Failed to fetch: {url}")
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.get_text(strip=True) if soup.title else None
        tables = extract_tables(html)
        return {"url": url, "title": title, "tables": tables}
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Network error: {str(e)}")


# -----------------------------
# Models
# -----------------------------

class ScrapeRequest(BaseModel):
    url: str
    crawl: bool = False
    max_pages: int = 10


# -----------------------------
# Routes
# -----------------------------

@app.get("/")
def read_root():
    return {"message": "Ball TD conversions API is running"}


@app.get("/api/hello")
def hello():
    return {"message": "Hello from the backend API!"}


@app.get("/test")
def test_database():
    response = {
        "backend": "✅ Running",
        "database": "❌ Not Available",
        "database_url": None,
        "database_name": None,
        "connection_status": "Not Connected",
        "collections": []
    }

    try:
        if db is not None:
            response["database"] = "✅ Available"
            response["database_url"] = "✅ Configured"
            response["database_name"] = db.name if hasattr(db, 'name') else "✅ Connected"
            response["connection_status"] = "Connected"
            try:
                collections = db.list_collection_names()
                response["collections"] = collections[:10]
                response["database"] = "✅ Connected & Working"
            except Exception as e:
                response["database"] = f"⚠️  Connected but Error: {str(e)[:50]}"
        else:
            response["database"] = "⚠️  Available but not initialized"
    except Exception as e:
        response["database"] = f"❌ Error: {str(e)[:50]}"

    import os as _os
    response["database_url"] = "✅ Set" if _os.getenv("DATABASE_URL") else "❌ Not Set"
    response["database_name"] = "✅ Set" if _os.getenv("DATABASE_NAME") else "❌ Not Set"

    return response


@app.post("/api/scrape")
def scrape(req: ScrapeRequest):
    if db is None:
        raise HTTPException(status_code=500, detail="Database not available")

    start_url = clean_url(req.url)
    pages_saved = 0
    visited: Set[str] = set()
    to_visit: List[str] = [start_url]

    base_origin = urlparse(start_url).netloc

    while to_visit and (not req.crawl or pages_saved < max(1, req.max_pages)):
        current = clean_url(to_visit.pop(0))
        if current in visited:
            continue
        visited.add(current)

        data = fetch_page(current)

        # Upsert into MongoDB
        record = {
            "url": data["url"],
            "path": urlparse(data["url"]).path,
            "title": data.get("title"),
            "tables": data.get("tables", []),
        }
        db["scrapepage"].update_one({"url": record["url"]}, {"$set": record, "$currentDate": {"scraped_at": True}}, upsert=True)
        pages_saved += 1

        if req.crawl:
            # collect same-origin links
            try:
                resp = requests.get(current, timeout=15)
                soup = BeautifulSoup(resp.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a.get("href")
                    abs_url = urljoin(current, href)
                    abs_url = clean_url(abs_url)
                    if urlparse(abs_url).netloc == base_origin and abs_url not in visited:
                        if abs_url not in to_visit and abs_url.startswith("http"):
                            to_visit.append(abs_url)
            except Exception:
                pass

    return {"status": "ok", "pages_saved": pages_saved}


@app.get("/api/pages")
def list_pages(limit: int = Query(100, ge=1, le=1000)):
    if db is None:
        raise HTTPException(status_code=500, detail="Database not available")
    docs = db["scrapepage"].find({}, {"tables": {"$slice": 0}}).limit(limit)
    results = []
    for d in docs:
        results.append({
            "id": str(d.get("_id")),
            "url": d.get("url"),
            "path": d.get("path"),
            "title": d.get("title"),
            "table_count": len(d.get("tables", []))
        })
    return {"items": results}


@app.get("/api/page")
def get_page(url: Optional[str] = None, id: Optional[str] = None):
    if db is None:
        raise HTTPException(status_code=500, detail="Database not available")
    if not url and not id:
        raise HTTPException(status_code=400, detail="Provide url or id")

    query: Dict[str, Any]
    if url:
        query = {"url": clean_url(url)}
    else:
        from bson import ObjectId
        try:
            query = {"_id": ObjectId(id)}
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid id")

    doc = db["scrapepage"].find_one(query)
    if not doc:
        raise HTTPException(status_code=404, detail="Page not found")

    # Convert ObjectId
    doc["id"] = str(doc.pop("_id", ""))
    return doc


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
