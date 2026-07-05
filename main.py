import os
import time
import asyncio
import re
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
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

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
MAL_CONCURRENT = 2

page_semaphore = asyncio.Semaphore(PAGE_CONCURRENT)
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
# AnimePahe fetcher
# ----------------------------
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=20)
)
async def get_animepahe(jikan_data: dict) -> str | None:
    try:
        def normalize(t: str) -> str:
            return re.sub(r'[^a-z0-9]', '', t.strip().lower())

        title_default = jikan_data.get("title")
        title_english = jikan_data.get("title_english")

        if not title_default:
            return None

        # Cache check
        cache_key = (
            jikan_data.get("mal_id")
            or title_english
            or title_default
        )

        cache_entry = animepahe_cache.get(cache_key)
        if (
            cache_entry
            and time.time() - cache_entry["timestamp"]
            < CACHE_EXPIRATION
        ):
            return cache_entry["data"]

        resp = await HTTP_CLIENT.get(
            "https://animepahe-sessions.vercel.app/animepahe.json"
        )

        if resp.status_code != 200:
            print(
                f"AnimePahe sessions returned "
                f"{resp.status_code}"
            )
            return None

        animes = resp.json()

        # 1. Try English title first
        if title_english:
            target = normalize(title_english)

            for anime in animes:
                if normalize(anime.get("title", "")) == target:
                    session = anime.get("session")

                    animepahe_cache[cache_key] = {
                        "data": session,
                        "timestamp": time.time()
                    }

                    return session

        # 2. Fallback Romaji
        target = normalize(title_default)

        for anime in animes:
            if normalize(anime.get("title", "")) == target:
                session = anime.get("session")

                animepahe_cache[cache_key] = {
                    "data": session,
                    "timestamp": time.time()
                }

                return session

        return None

    except Exception as e:
        print("AnimePahe fetch error:", e)
        return None


async def check_netflix_tmdb(title: str) -> bool:
    try:
        def extract_season_number(title: str):
            patterns = [
                r'(\d+)(?:st|nd|rd|th)\s+season',
                r'season\s+(\d+)',
                r'\bs(\d+)\b',
                r'part\s+(\d+)',
                r'(\d+)(?:st|nd|rd|th)\s+part',
                r'cour\s+(\d+)',
                r'(\d+)(?:st|nd|rd|th)\s+cour'
            ]

            for pattern in patterns:
                m = re.search(
                    pattern,
                    title,
                    re.IGNORECASE
                )
                if m:
                    return int(m.group(1))

            roman_map = {
                "ii": 2,
                "iii": 3,
                "iv": 4,
                "v": 5,
                "vi": 6
            }

            m = re.search(
                r'\b(ii|iii|iv|v|vi)\b',
                title,
                re.IGNORECASE
            )

            if m:
                return roman_map[
                    m.group(1).lower()
                ]

            return None

        season_number = extract_season_number(
            title
        )

        search_title = title

        if season_number:
            patterns_to_remove = [
                r'(\d+)(?:st|nd|rd|th)\s+season',
                r'season\s+\d+',
                r'\bs\d+\b',
                r'part\s+\d+',
                r'(\d+)(?:st|nd|rd|th)\s+part',
                r'part\s+(ii|iii|iv|v|vi)\b',
                r'cour\s+\d+',
                r'(\d+)(?:st|nd|rd|th)\s+cour',
                r'\bii\b',
                r'\biii\b',
                r'\biv\b',
                r'\bv\b',
                r'\bvi\b'
            ]

            for pattern in patterns_to_remove:
                search_title = re.sub(
                    pattern,
                    '',
                    search_title,
                    flags=re.IGNORECASE
                )

            search_title = re.sub(
                r'\s+',
                ' ',
                search_title
            ).strip()

        async with httpx.AsyncClient(
            timeout=10
        ) as client:

            tasks = [
                client.get(
                    f"https://api.themoviedb.org/3/search/{media_type}",
                    params={
                        "api_key": TMDB_API_KEY,
                        "query": search_title
                    }
                )
                for media_type in [
                    "tv",
                    "movie"
                ]
            ]

            responses = await asyncio.gather(
                *tasks
            )

            for media_type, resp in zip(
                ["tv", "movie"],
                responses
            ):
                if resp.status_code != 200:
                    continue

                results = (
                    resp.json()
                    .get("results", [])
                )

                if not results:
                    continue

                # Check multiple possible matches
                for result in results[:5]:
                    item_id = result["id"]

                    tmdb_title = (
                        result.get("name")
                        or result.get("title")
                        or ""
                    )

                    # Prefer exact match
                    if (
                        tmdb_title.lower()
                        != search_title.lower()
                        and results.index(result) > 0
                    ):
                        continue

                    # -----------------------------------
                    # Verify season exists
                    # -----------------------------------
                    if (
                        media_type == "tv"
                        and season_number is not None
                    ):
                        tv_resp = await client.get(
                            f"https://api.themoviedb.org/3/tv/{item_id}",
                            params={
                                "api_key": TMDB_API_KEY
                            }
                        )

                        if tv_resp.status_code != 200:
                            continue

                        seasons = (
                            tv_resp.json()
                            .get("seasons", [])
                        )

                        season_exists = any(
                            season.get(
                                "season_number"
                            ) == season_number
                            for season in seasons
                        )

                        # Handle TMDB merged seasons.
                        # Example:
                        # Apothecary Diaries S1+S2 = 48 eps.
                        if (
                            not season_exists
                            and season_number > 1
                        ):
                            real_seasons = [
                                s
                                for s in seasons
                                if s.get(
                                    "season_number",
                                    0
                                ) > 0
                            ]

                            if len(real_seasons) <= 1:
                                season_exists = True

                        if not season_exists:
                            continue

                    # -----------------------------------
                    # Netflix PH provider check
                    # -----------------------------------
                    wp_resp = await client.get(
                        f"https://api.themoviedb.org/3/{media_type}/{item_id}/watch/providers",
                        params={
                            "api_key": TMDB_API_KEY
                        }
                    )

                    if wp_resp.status_code != 200:
                        continue

                    ph_data = (
                        wp_resp.json()
                        .get("results", {})
                        .get("PH", {})
                    )

                    for provider in ph_data.get(
                        "flatrate",
                        []
                    ):
                        if (
                            provider.get(
                                "provider_name",
                                ""
                            ).lower()
                            == "netflix"
                        ):
                            return True

        return False

    except Exception as e:
        print(
            f"TMDB error for '{title}': {e}"
        )
        return False

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

            # Parse aired date
            aired_from = None
            from_prop = (
                data.get("aired", {})
                .get("prop", {})
                .get("from")
            )

            if (
                from_prop
                and from_prop.get("year")
                and from_prop.get("month")
                and from_prop.get("day")
            ):
                from datetime import datetime

                aired_from = datetime(
                    from_prop["year"],
                    from_prop["month"],
                    from_prop["day"]
                ).date().isoformat()

            broadcast_info = data.get("broadcast", {}) or {}

            broadcast = {
                "day": broadcast_info.get("day"),
                "time": broadcast_info.get("time"),
                "timezone": broadcast_info.get("timezone"),
                "string": broadcast_info.get("string")
            }

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

        netflix_available = await check_netflix_tmdb(title)

        animepahe_UUID = await get_animepahe(data)

        return {
            "episodes": episodes,
            "mal_score": score,
            "animepahe_UUID": animepahe_UUID,
            "netflix_available": netflix_available,
            "aired": aired_from,
            "broadcast": broadcast
        }


# ----------------------------
# Batch update endpoint
# ----------------------------

BATCH_SIZE = 25

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
            airing_status = (
                (
                    props.get("Airing Status", {})
                    .get("formula", {})
                    .get("string")
                )
                or ""
            ).strip()

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

            # Broadcast Infos
            broadcast = anime_info.get("broadcast", {})

            if airing_status.lower() == "pending":

                # Aired Date
                new_aired = anime_info.get("aired")
                current_aired = (
                    (props.get("Aired", {}).get("date") or {})
                    .get("start")
                )

                if new_aired and current_aired != new_aired:
                    updates["Aired"] = {
                        "date": {
                            "start": new_aired
                        }
                    }

                # Air Day (Select)
                new_day = broadcast.get("day")
                current_day = (
                    (props.get("Air Day", {}).get("select") or {})
                    .get("name")
                )

                if new_day and current_day != new_day:
                    updates["Air Day"] = {
                        "select": {
                            "name": new_day
                        }
                    }

                # Air Time
                new_air_time = broadcast.get("time")
                current_air_time_rich = (
                    props.get("Air Time", {})
                    .get("rich_text", [])
                )

                current_air_time = (
                    current_air_time_rich[0]["plain_text"]
                    if current_air_time_rich
                    else None
                )

                if new_air_time and current_air_time != new_air_time:
                    updates["Air Time"] = {
                        "rich_text": [
                            {
                                "text": {
                                    "content": new_air_time
                                }
                            }
                        ]
                    }

                # Timezone
                new_timezone = broadcast.get("timezone")
                current_timezone_rich = (
                    props.get("Timezone", {})
                    .get("rich_text", [])
                )

                current_timezone = (
                    current_timezone_rich[0]["plain_text"]
                    if current_timezone_rich
                    else None
                )

                if new_timezone and current_timezone != new_timezone:
                    updates["Timezone"] = {
                        "rich_text": [
                            {
                                "text": {
                                    "content": new_timezone
                                }
                            }
                        ]
                    }

            # MAL Score
            mal_score_rich = props.get("MAL Score", {}).get("rich_text", [])
            current_score_raw = mal_score_rich[0].get("plain_text") if mal_score_rich else None

            def parse_score(s):
                if not s or s.strip() in ["0.00 ★", ""]:
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

            # Netflix Availability
            current_netflix = (
                props.get("Netflix Availability", {})
                .get("checkbox", False)
            )

            new_netflix = anime_info.get("netflix_available")

            if new_netflix != current_netflix:
                updates["Netflix Availability"] = {
                    "checkbox": new_netflix
                }

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