import os
import time
import asyncio
import re
import time
from urllib.parse import quote_plus
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi import Query
from fastapi.responses import JSONResponse
from tenacity import retry, stop_after_attempt, wait_exponential

# ----------------------------
# Load environment variables
# ----------------------------
load_dotenv()
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DATABASE_ANIME_ID = os.getenv("NOTION_DATABASE_ANIME_ID")
NOTION_PAGE_ANIMEPICKER_ID = os.getenv("NOTION_PAGE_ANIMEPICKER_ID")

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
PAGE_CONCURRENT = 4  # lower concurrency for accuracy
ANIMEPAHE_CONCURRENT = 2
MAL_CONCURRENT = 2

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
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ANIME_ID}/query"
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
# AnimePahe fetcher (UPDATED LOGIC)
# ----------------------------
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=20))
async def get_animepahe(jikan_data: dict) -> str | None:
    try:
        def normalize(t: str) -> str:
            return re.sub(r'[^a-z0-9]', '', t.strip().lower())

        # ----------------------------
        # Extract titles (like your new version)
        # ----------------------------
        title_default = jikan_data.get("title")  # ROMAJI
        title_english = jikan_data.get("title_english")

        if not title_default:
            return None

        # ----------------------------
        # CACHE CHECK (kept, but now uses romaji key stability)
        # ----------------------------
        cache_key = jikan_data.get("mal_id") or (title_english or title_default)
        cache_entry = animepahe_cache.get(cache_key)
        if cache_entry and time.time() - cache_entry["timestamp"] < CACHE_EXPIRATION:
            return cache_entry["data"]

        async with animepahe_semaphore:

            query = quote_plus(title_default)
            url = f"https://animepahe.pw/api?m=search&q={query}"

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

            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, headers=headers)

            if resp.status_code != 200:
                print(f"AnimePahe API returned {resp.status_code}")
                return None

            data = resp.json()
            results = data.get("data", [])
            if not results:
                return None

            # 1. Try English match first
            if title_english:
                target = normalize(title_english)

                for result in results:
                    anime_title = normalize(result.get("title", ""))
                    if anime_title == target:
                        session = result.get("session")
                        animepahe_cache[cache_key] = {
                            "data": session,
                            "timestamp": time.time()
                        }
                        return session

            # 2. Fallback Romaji match
            target_romaji = normalize(title_default)

            for result in results:
                anime_title = normalize(result.get("title", ""))
                if anime_title == target_romaji:
                    session = result.get("session")
                    animepahe_cache[cache_key] = {
                        "data": session,
                        "timestamp": time.time()
                    }
                    return session

            return None

    except Exception as e:
        print("AnimePahe fetch error:", e)
        return None

# ----------------------------
# MAL fetcher with retry (FULL + fallback)
# ----------------------------
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=20))
async def get_anime_info_from_mal_id(mal_id: str) -> dict:
    async with mal_semaphore:
        await asyncio.sleep(0.1)

        try:
            # ----------------------------
            # Try FULL endpoint first
            # ----------------------------
            resp = await HTTP_CLIENT.get(f"https://api.jikan.moe/v4/anime/{mal_id}/full")

            if resp.status_code != 200:

                # ----------------------------
                # Fallback endpoint (NO /full)
                # ----------------------------
                resp = await HTTP_CLIENT.get(f"https://api.jikan.moe/v4/anime/{mal_id}")

                if resp.status_code != 200:
                    raise Exception(f"MAL API failed with status {resp.status_code}")

            data = resp.json().get("data") or {}

        except httpx.RequestError as e:
            raise Exception(f"Jikan request error: {str(e)}")

        # ----------------------------
        # Safe parsing
        # ----------------------------
        episodes = data.get("episodes")

        score_val = data.get("score")
        score = (
            f"{score_val:.2f} ★"
            if isinstance(score_val, (int, float))
            else None
        )

        title = data.get("title_english") or data.get("title") or "Unknown"

        animepahe_UUID = await get_animepahe(data)

        return {
            "episodes": episodes,
            "mal_score": score,
            "animepahe_UUID": animepahe_UUID
        }


# ----------------------------
# Batch update endpoint (accuracy-focused, with dry-run & offset)
# ----------------------------

BATCH_SIZE = 15

# Notion helper for automation_index
async def get_automation_index():
    url = f"https://api.notion.com/v1/pages/{NOTION_PAGE_ANIMEPICKER_ID}"
    resp = await HTTP_CLIENT.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"Failed to fetch automation_index: {resp.status_code}")
        return 0
    data = resp.json()
    try:
        return data["properties"]["automation_index"]["number"] or 0
    except KeyError:
        return 0

async def set_automation_index(value: int):
    url = f"https://api.notion.com/v1/pages/{NOTION_PAGE_ANIMEPICKER_ID}"
    payload = {"properties": {"automation_index": {"number": value}}}
    await HTTP_CLIENT.patch(url, headers=HEADERS, json=payload)

@app.get("/batch-update-animes/")
async def batch_update_animes(dry_run: bool = Query(False, description="If True, do not update Notion, just simulate")):
    pages = await fetch_notion_pages()
    total_pages = len(pages)
    results = []
    processed_count = 0
    start_time = time.perf_counter()

    if total_pages == 0:
        return {"total": 0, "results": [], "elapsed_seconds": 0}

    # Fetch current automation_index
    batch_offset = await get_automation_index()
    start_idx = batch_offset
    end_idx = min(batch_offset + BATCH_SIZE, total_pages)
    current_batch = pages[start_idx:end_idx]

    print(f"Processing pages {start_idx + 1} to {end_idx} of {total_pages} (dry_run={dry_run})")

    # ----------------------------
    # Process each page in this batch
    # ----------------------------
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

            print(f"[{index + 1}/{len(current_batch)}] Processing: {title} (MAL ID: {mal_id})")

            if not mal_id:
                results.append({"title": title, "updated": None, "reason": "missing mal_id"})
                processed_count += 1
                return

            # Fetch anime info
            try:
                anime_info = await get_anime_info_from_mal_id(mal_id)
            except Exception as e:
                results.append({"title": title, "updated": None, "reason": f"fetch failed: {e}"})
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

            # AnimePahe UUID
            new_uuid = anime_info.get("animepahe_UUID")
            current_uuid_rich = props.get("AnimepaheUUID", {}).get("rich_text", [])
            current_uuid = current_uuid_rich[0]["plain_text"] if current_uuid_rich else None

            if new_uuid and (not current_uuid or current_uuid != new_uuid):
                updates["AnimepaheUUID"] = {"rich_text": [{"text": {"content": new_uuid}}]}

            # Apply updates if not dry_run
            if updates:
                if not dry_run:
                    await HTTP_CLIENT.patch(
                        f"https://api.notion.com/v1/pages/{page_id}",
                        headers=HEADERS,
                        json={"properties": updates}
                    )
                results.append({"title": title, "updated": updates})
                update_preview = {k: (v["rich_text"][0]["text"]["content"] if "rich_text" in v else v) for k, v in updates.items()}
                print(f"[{index + 1}/{len(current_batch)}] Updates applied: {update_preview}" + (" (dry_run)" if dry_run else ""))
            else:
                results.append({"title": title, "updated": None, "reason": "no changes"})
                print(f"[{index + 1}/{len(current_batch)}] No changes needed")

            processed_count += 1

    # Run concurrently
    await asyncio.gather(*(process_page(page, idx) for idx, page in enumerate(current_batch)))

    # Update automation_index for next run (always)
    new_offset = batch_offset + BATCH_SIZE
    if new_offset >= total_pages:
        new_offset = 0  # wrap around

    # Always update, regardless of dry_run
    await set_automation_index(new_offset)

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Batch update complete: processed {processed_count}/{len(current_batch)} pages in {elapsed_time:.2f} seconds.")

    return {
        "total": total_pages,
        "batch_processed": len(current_batch),
        "results": results,
        "dry_run": dry_run,
        "next_start_index": new_offset, 
        "elapsed_seconds": round(elapsed_time, 2)
    }