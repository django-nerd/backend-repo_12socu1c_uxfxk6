import os
import re
from datetime import datetime
from typing import List, Optional, Set, Dict, Any, Tuple
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from database import db, create_document
from schemas import ConversionRecord

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
        return {"url": url, "title": title, "html": html, "tables": tables}
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Network error: {str(e)}")


# Lightweight OCR surrogate: use image alt/title/aria-label and nearby captions

def collect_text_candidates(soup: BeautifulSoup, ocr: bool = False) -> List[str]:
    lines: List[str] = []
    # Headings and paragraphs
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "span", "div"]):
        txt = tag.get_text(" ", strip=True)
        if txt and len(txt) >= 2:
            lines.append(txt)
    # Optional: image metadata
    if ocr:
        for img in soup.find_all("img"):
            for attr in ("alt", "title", "aria-label"):
                val = img.get(attr)
                if val and len(val.strip()) > 2:
                    lines.append(val.strip())
            # look for sibling caption-like text
            parent = img.parent
            for sib in (parent.find_next_siblings() if parent else []):
                st = sib.get_text(" ", strip=True)
                if st:
                    lines.append(st)
                    break
    # Deduplicate while preserving order
    seen = set()
    uniq = []
    for l in lines:
        if l not in seen:
            uniq.append(l)
            seen.add(l)
    return uniq


def parse_rate_from_match(m: re.Match) -> Tuple[str, str, float]:
    # Normalize commas to dots and cast
    def num(x: str) -> float:
        return float(x.replace(",", "."))

    if m.lastgroup == "eq":
        a1 = num(m.group("a1"))
        s = m.group("s").strip()
        a2 = num(m.group("a2"))
        t = m.group("t").strip()
        if a1 == 0:
            raise ValueError("zero source amount")
        rate = a2 / a1
        return s, t, rate
    elif m.lastgroup == "one":
        s = m.group("s").strip()
        a2 = num(m.group("a2"))
        t = m.group("t").strip()
        return s, t, a2
    elif m.lastgroup == "ratio":
        s = m.group("s").strip()
        t = m.group("t").strip()
        a1 = num(m.group("a1"))
        a2 = num(m.group("a2"))
        if a1 == 0:
            raise ValueError("zero source amount")
        rate = a2 / a1
        return s, t, rate
    else:
        raise ValueError("unknown pattern")


def extract_conversions(url: str, title: Optional[str], html: str, ocr: bool = False) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    lines = collect_text_candidates(soup, ocr=ocr)

    # Regex patterns for common styles
    # 1) "1 Gem = 100 Coins" or "2 Gems = 300 Coins"
    p_eq = re.compile(r"(?P<a1>\d+(?:[.,]\d+)?)\s*(?:x|×)?\s*(?P<s>[A-Za-z][A-Za-z \-]*)\s*(?:=|=>|→|to)\s*(?P<a2>\d+(?:[.,]\d+)?)\s*(?P<t>[A-Za-z][A-Za-z \-]*)", re.IGNORECASE)
    # 2) "1 Gem to 100 Coin" handled by eq
    # 3) Ratio style: "Gem to Coin: 1:100" or "Gem→Coin 1:100"
    p_ratio = re.compile(r"(?P<s>[A-Za-z][A-Za-z \-]*)\s*(?:to|→|->|:)\n\s*(?P<t>[A-Za-z][A-Za-z \-]*)\s*[:\- ]\s*(?P<a1>\d+(?:[.,]\d+)?)\s*[:/]\s*(?P<a2>\d+(?:[.,]\d+)?)", re.IGNORECASE)
    # 4) "1 Gem = 100" with inferred target from nearby context is hard; skip.

    results: List[Dict[str, Any]] = []
    for line in lines:
        # try equal pattern
        m = p_eq.search(line)
        if m:
            try:
                s, t, rate = parse_rate_from_match(m)
                results.append({"source": s.strip(), "target": t.strip(), "rate": float(rate), "text": line, "page_url": url, "page_title": title})
                continue
            except Exception:
                pass
        # try ratio pattern
        m2 = p_ratio.search(line)
        if m2:
            try:
                s, t, rate = parse_rate_from_match(m2)
                results.append({"source": s.strip(), "target": t.strip(), "rate": float(rate), "text": line, "page_url": url, "page_title": title})
                continue
            except Exception:
                pass

    # Post-process: normalize multi-spaces
    for r in results:
        r["source"] = re.sub(r"\s+", " ", r["source"]).strip().title()
        r["target"] = re.sub(r"\s+", " ", r["target"]).strip().title()
    # Deduplicate by (page_url, source, target, rate)
    uniq: Dict[Tuple[str, str, str, float], Dict[str, Any]] = {}
    for r in results:
        key = (r["page_url"], r["source"], r["target"], r["rate"])
        if key not in uniq:
            uniq[key] = r
    return list(uniq.values())


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
        url = d.get("url")
        conv_count = 0
        try:
            conv_count = db["conversion"].count_documents({"page_url": url})
        except Exception:
            conv_count = 0
        results.append({
            "id": str(d.get("_id")),
            "url": url,
            "path": d.get("path"),
            "title": d.get("title"),
            "table_count": len(d.get("tables", [])),
            "conversion_count": conv_count,
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


# -------- Conversion endpoints --------

@app.post("/api/extract")

def extract_conversions_endpoint(payload: Dict[str, Any]):
    if db is None:
        raise HTTPException(status_code=500, detail="Database not available")
    url = payload.get("url")
    pid = payload.get("id")
    ocr = bool(payload.get("ocr", False))
    if not url and not pid:
        raise HTTPException(status_code=400, detail="Provide url or id")

    # Fetch page record and html
    page_doc = None
    if url:
        page_doc = db["scrapepage"].find_one({"url": clean_url(url)})
    else:
        from bson import ObjectId
        try:
            page_doc = db["scrapepage"].find_one({"_id": ObjectId(pid)})
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid id")

    html = None
    page_url = None
    page_title = None
    if page_doc:
        page_url = page_doc.get("url")
        page_title = page_doc.get("title")
        # We don't store HTML; fetch it live
        try:
            fetched = fetch_page(page_url)
            html = fetched.get("html")
            page_title = page_title or fetched.get("title")
        except HTTPException as e:
            raise e
    else:
        # If not in DB, fetch directly
        if not url:
            raise HTTPException(status_code=404, detail="Page not found")
        fetched = fetch_page(url)
        page_url = fetched["url"]
        page_title = fetched.get("title")
        html = fetched.get("html")

    items = extract_conversions(page_url, page_title, html, ocr=ocr)

    # Upsert into collection
    upserts = 0
    for it in items:
        now = datetime.utcnow()
        q = {"page_url": it["page_url"], "source": it["source"], "target": it["target"]}
        update = {"$set": {"rate": it["rate"], "text": it.get("text"), "page_title": it.get("page_title"), "updated_at": now},
                  "$setOnInsert": {"created_at": now}}
        db["conversion"].update_one(q, update, upsert=True)
        upserts += 1

    return {"status": "ok", "count": len(items), "upserts": upserts, "items": items}


@app.get("/api/conversions")

def list_conversions(page_url: Optional[str] = None, limit: int = Query(200, ge=1, le=1000)):
    if db is None:
        raise HTTPException(status_code=500, detail="Database not available")
    query: Dict[str, Any] = {}
    if page_url:
        query["page_url"] = clean_url(page_url)
    cur = db["conversion"].find(query).limit(limit)
    items: List[Dict[str, Any]] = []
    for d in cur:
        d["id"] = str(d.pop("_id", ""))
        items.append(d)
    return {"items": items}


@app.post("/api/conversions/upsert")

def upsert_conversions(payload: Dict[str, Any]):
    if db is None:
        raise HTTPException(status_code=500, detail="Database not available")
    page_url = payload.get("page_url")
    page_title = payload.get("page_title")
    items = payload.get("items", [])
    if not page_url:
        raise HTTPException(status_code=400, detail="page_url is required")
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="items must be a non-empty list")

    upserts = 0
    for it in items:
        try:
            source = str(it["source"]).strip()
            target = str(it["target"]).strip()
            rate = float(it["rate"])
        except Exception:
            continue
        now = datetime.utcnow()
        q = {"page_url": clean_url(page_url), "source": source, "target": target}
        update = {"$set": {"rate": rate, "text": it.get("text"), "page_title": page_title or it.get("page_title"), "updated_at": now},
                  "$setOnInsert": {"created_at": now}}
        db["conversion"].update_one(q, update, upsert=True)
        upserts += 1

    return {"status": "ok", "upserts": upserts}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
