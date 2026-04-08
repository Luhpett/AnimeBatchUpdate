import os
import time
import asyncio
import re
import time
from urllib.parse import quote_plus
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from tenacity import retry, stop_after_attempt, wait_exponential

# ----------------------------
# Load environment variables
# ----------------------------
load_dotenv()
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

# ----------------------------
# FastAPI setup
# ----------------------------
app = FastAPI()

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

class PrettyJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        import json
        return json.dumps(content, indent=2, ensure_ascii=False).encode("utf-8")

app.default_response_class = PrettyJSONResponse

# ----------------------------
# Global HTTP client (reuse)
# ----------------------------
HTTP_CLIENT = httpx.AsyncClient(
    timeout=60,  # increased timeout for reliability
    limits=httpx.Limits(max_connections=50, max_keepalive_connections=25)
)

# ----------------------------
# Concurrency semaphores
# ----------------------------
PAGE_CONCURRENT = 2  # lower concurrency for accuracy
ANIMEPAHE_CONCURRENT = 1
MAL_CONCURRENT = 1

page_semaphore = asyncio.Semaphore(PAGE_CONCURRENT)
animepahe_semaphore = asyncio.Semaphore(ANIMEPAHE_CONCURRENT)
mal_semaphore = asyncio.Semaphore(MAL_CONCURRENT)

# ----------------------------
# AnimePahe cache with expiration
# ----------------------------
CACHE_EXPIRATION = 3600  # 1 hour
animepahe_cache = {}

# ----------------------------
# Helpers
# ----------------------------
def normalize_title(title: str) -> str:
    title = title.lower()
    title = re.sub(r'[^a-z0-9 ]', '', title)
    title = re.sub(r'\b(season|part|cour|s)\s*\d+\b', '', title)
    return title.strip()

def similarity(a: str, b: str) -> int:
    return len(set(a.split()) & set(b.split()))

# ----------------------------
# Notion helpers
# ----------------------------
async def fetch_notion_pages():
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    pages = []
    has_more = True
    cursor = None
    while has_more:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        resp = await HTTP_CLIENT.post(url, headers=HEADERS, json=payload)
        data = resp.json()
        pages.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        cursor = data.get("next_cursor")
    return pages

# ----------------------------
# AnimePahe fetcher with retry
# ----------------------------
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=20))
async def get_animepahe(title: str) -> str | None:
    # Check cache
    cache_entry = animepahe_cache.get(title)
    if cache_entry and time.time() - cache_entry['timestamp'] < CACHE_EXPIRATION:
        return cache_entry['data']

    async with animepahe_semaphore:
        # await asyncio.sleep(0.1)
        title_clean = title.strip().lower()
        season_match = re.search(r'(?:season|part|cour|s)\s*(\d+)', title, re.IGNORECASE)
        season_number = int(season_match.group(1)) if season_match else None
        query = quote_plus(title)
        url = f"https://animepahe.pw/api?m=search&q={query}"

        # Full headers to ensure AnimePahe accepts the request
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.5",
            "Cache-Control": "max-age=0",
            "Cookie": "__ddgid_=Qny5tqhn2IuJLwux; __ddg2_=EYn9n6HAE66tSl1U; __ddg1_=YEsiZtjZxa7skftDfVwd; latest=6295; res=1080; aud=jpn; av1=0; __ddg9_=136.158.56.81; __ddg8_=rY5ZNuF3hi99l8iZ; __ddg10_=1761295866; XSRF-TOKEN=eyJpdiI6ImVnOTVQMXFrWFBkNWdRY3ZlSDFwOUE9PSIsInZhbHVlIjoiVkdBNXRuVlVMNU5scHRWNWRxOHA5bU10QVIrbHVsaGpqWHZTT1d6cWFCL1NaVkVZWW1seS9QZkUrNGJOZm0wZUF6M3VFSFVmdGdZbVp6ZzBGbGxUVkRLcXYxeWw2QkYwTzNVUDBQdkhVTFpWUlRzekZLcThBcW5PNlQ1YmlOOHYiLCJtYWMiOiIxMjAwYTU0MGE4MDFlZWQzZjYyODkyYjI3MGYxODdlZTA1MjIwZGFlNTJhZmRkMzg4MGQ0NzYxMWQ1MDM4MjRiIiwidGFnIjoiIn0%3D; laravel_session=eyJpdiI6Ijh4TVlydE5NZXUxSXhpZFkzcGRjSmc9PSIsInZhbHVlIjoiTkprakxkaXB4QUFtL1Vzcm1iVTJ4eFpVdnNmaDNMMitEUGFTaEM3emtvRnB0bmVXZmpqVGY4VExDbzYrdWFvREZmM1BNdjlYbFV1aHVTOGNaOUFxTEJIS0gvRXd1b0dydHZld3BaRm9wRk1PZUdtSEVLWjI0QlRaZlNROXdhSkQiLCJtYWMiOiJlZjAyMjIxZDEzZDM1ZjViY2NmZmI1NDA4Njc1NzhkYTRhZDU3NmYxNDY1NDNlNGQzNjQ5M2ZjZDYwOGY2ZDk3IiwidGFnIjoiIn0%3D",
            "Referer": "https://animepahe.si/",
            "Origin": "https://animepahe.si",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Sec-GPC": "1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1",
        }

        resp = await HTTP_CLIENT.get(url, headers=headers)
        if resp.status_code != 200:
            print(f"AnimePahe returned {resp.status_code} for {title}")
            return None

        data = resp.json()
        results = data.get("data", [])
        if not results:
            return None

        # Pick the best match
        best_score = 0
        best_session = None
        MIN_SCORE = 5

        for result in results:
            anime_title = result.get("title", "").lower()
            score = 0
            if title_clean in anime_title or anime_title in title_clean:
                score += 5
            if season_number:
                season_in_title = re.search(r'(?:season|part|cour|s)\s*(\d+)', anime_title, re.IGNORECASE)
                if season_in_title and int(season_in_title.group(1)) == season_number:
                    score += 10
            if score > best_score:
                best_score = score
                best_session = result.get("session")

        if best_score >= MIN_SCORE:
            animepahe_cache[title] = {'data': best_session, 'timestamp': time.time()}
            return best_session

        return None

# ----------------------------
# MAL fetcher with retry
# ----------------------------
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=20))
async def get_anime_info_from_mal_id(mal_id: str) -> dict:
    async with mal_semaphore:
        await asyncio.sleep(0.1)
        resp = await HTTP_CLIENT.get(f"https://api.jikan.moe/v4/anime/{mal_id}/full")
        if resp.status_code != 200:
            raise Exception(f"MAL API failed with status {resp.status_code}")
        data = resp.json()["data"]
        episodes = data.get("episodes")
        score = f"{data['score']:.2f} ★" if data.get("score") else None
        title = data.get("title_english") or data.get("title")
        animepahe_UUID = await get_animepahe(title)
        return {"episodes": episodes, "mal_score": score, "animepahe_UUID": animepahe_UUID}

# ----------------------------
# Batch update endpoint (accuracy-focused)
# ----------------------------
@app.get("/batch-update-animes/")
async def batch_update_animes_dry():
    pages = await fetch_notion_pages()
    results = []
    total_pages = len(pages)
    processed_count = 0

    start_time = time.perf_counter()  # Start timing

    async def process_page(page, index):
        nonlocal processed_count
        async with page_semaphore:
            await asyncio.sleep(0.1)
            props = page.get("properties", {})
            page_id = page.get("id")

            # Get MAL ID
            mal_id = None
            mal_prop = props.get("mal_id")
            if mal_prop and mal_prop.get("type") == "formula":
                try:
                    mal_id = int(mal_prop["formula"].get("number"))
                except (TypeError, ValueError):
                    mal_id = None

            # Get title
            title_prop = props.get("Name") or props.get("Title")
            title = (title_prop.get("title")[0]["plain_text"].strip()
                     if title_prop and title_prop.get("title") else "Unknown")

            print(f"[{index + 1}/{total_pages}] Processing: {title} (MAL ID: {mal_id})")

            if not mal_id:
                results.append({"title": title, "would_update": None, "reason": "missing mal_id"})
                processed_count += 1
                return

            # Fetch anime info
            try:
                anime_info = await get_anime_info_from_mal_id(mal_id)
            except Exception as e:
                results.append({"title": title, "would_update": None, "reason": f"fetch failed: {e}"})
                processed_count += 1
                return

            updates = {}

            # Episodes
            current_eps = props.get("Episodes", {}).get("number")
            new_eps = anime_info.get("episodes")
            if new_eps is not None and (current_eps is None or int(current_eps) != int(new_eps)):
                updates["Episodes"] = {"number": new_eps}

            # MAL Score
            mal_score_rich = props.get("MAL Score", {}).get("rich_text", [])
            current_score_raw = mal_score_rich[0].get("plain_text") if mal_score_rich else None

            def parse_score(s):
                if not s or s.strip() in ["N/A ★", ""]:
                    return None
                return float(s.strip().replace("★", "").strip())

            current_score = parse_score(current_score_raw)
            new_score = parse_score(anime_info.get("mal_score"))
            if new_score is not None and current_score != new_score:
                updates["MAL Score"] = {"rich_text": [{"text": {"content": f"{new_score:.2f} ★"}}]}

            # ----------------------------
            # AnimePahe UUID — force update if API returns a new one
            # ----------------------------
            new_uuid = anime_info.get("animepahe_UUID")
            if new_uuid:
                updates["AnimepaheUUID"] = {"rich_text": [{"text": {"content": new_uuid}}]}

            # ----------------------------
            # Log full updates with values
            # ----------------------------
            if updates:
                results.append({"title": title, "would_update": updates})

                # Prepare a readable dict for logging
                update_preview = {}
                for key, value in updates.items():
                    if "rich_text" in value:
                        update_preview[key] = value["rich_text"][0]["text"]["content"]
                    else:
                        update_preview[key] = value
                print(f"[{index + 1}/{total_pages}] Updates would be applied: {update_preview}")
            else:
                results.append({"title": title, "would_update": None, "reason": "no changes"})
                print(f"[{index + 1}/{total_pages}] No changes needed")

            processed_count += 1

    # Process pages concurrently
    await asyncio.gather(*(process_page(page, idx) for idx, page in enumerate(pages)))

    end_time = time.perf_counter()  # End timing
    elapsed_time = end_time - start_time
    print(f"Batch update dry-run complete: processed {processed_count}/{total_pages} pages in {elapsed_time:.2f} seconds.")

    return {
        "total": total_pages,
        "results": results,
        "elapsed_seconds": round(elapsed_time, 2)
    }