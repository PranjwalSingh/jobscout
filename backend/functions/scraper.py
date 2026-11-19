# Smart scraper approach using a convex hull-like strategy:
# Build a graph of all users' job-search configurations, cluster nearby configurations,
# and compute a convex hull of their combined requirements to define broader scraper runs.
# Each outer scraper run fetches a shared pool of jobs (e.g., 1,000 jobs),
# then individual user filters run underneath to extract their relevant subsets.
# This allows users to receive different numbers of jobs (e.g., 43, 59, or 200)
# from the same shared pool while minimizing redundant scraping and ensuring no user
# misses jobs due to overly narrow scraper configurations.
# right now it's one single scraper run that loops through the full matrix sequentially. 
# Later I could cluster the search matrix entries and fan out parallel scrapers per cluster
 
 
 
 
#  # IMPORTANT
#  great so i just need a backend endpoints managing, the different current users that are online and have them run the scraper on their time on the website, so as long as they are on, this should technically work right and the endpoint kinda just stores updates, on what user has what "start" running

# Exactly. Backend just needs to:

# Track which users are currently active
# Assign each a start offset
# Frontend polls the backend on load → gets assigned a start → runs that scrape silently → posts results back
# Backend increments the offset for next user

# Simple state like:

# json
# {
#   "current_start": 150,
#   "results_stored": 150
# }

# Each user picks up where the last left off. As long as you have traffic, scraping runs continuously for free.

# What’s your backend stack?

# fastapi

# Perfect. Something like:

# python
# # state (use Redis in prod)
# scrape_state = {"current_start": 0}

# @app.get("/scrape-task")
# async def get_scrape_task():
#     start = scrape_state["current_start"]
#     scrape_state["current_start"] += 25
#     return {"start": start}

# @app.post("/scrape-results")
# async def post_scrape_results(results: list[dict]):
#     # store to db
#     return {"ok": True}

# Frontend on load:

# js
# useEffect(() => {
#   const run = async () => {
#     const { start } = await fetch('/scrape-task').then(r => r.json())
#     const results = await fetch(`https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=...&start=${start}`)
#     await fetch('/scrape-results', { method: 'POST', body: JSON.stringify(results) })
#   }
#   run()
# }, [])

# Swap scrape_state for Redis when you scale. That’s basically it.

# rigth and this wont be problematic to user experience right nor would this be an issue in scraping ip u confirmed and also overall like we can track the current users reliably right with toekns i reckon that areonline?

# and also giev me a ltitle summary of our appraoch

# UX: One extra fetch in the background — negligible. User notices nothing.

# IP: Confirmed, requests go from their device, never your Vercel server.

# Tracking active users: JWT tokens work fine — just check if their token is valid/not expired when they hit /scrape-task. No token = no task assigned.

# Summary:

# Users load your React/Vite site on Vercel
# On page load, frontend silently hits your FastAPI /scrape-task endpoint
# Backend assigns a start offset (tracks progress in Redis)
# Frontend fetches that LinkedIn page directly from the user’s device IP
# Results posted back to FastAPI, stored in your DB
# Scraping is distributed across all active users’ IPs — no proxies, no Vercel involvement, no rate limit concentration

# Clean, free, scales with your traffic.
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND_DIR = PROJECT_ROOT / "backend"

for path in (str(PROJECT_ROOT), str(BACKEND_DIR)):
    if path not in sys.path:
        sys.path.insert(0, path)
import os
import re
import sys
import time
from datetime import datetime
from difflib import SequenceMatcher
import json, pathlib
from backend.routes.creation_dashboard import apply_config_filters
import requests
from bs4 import BeautifulSoup
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)



DATA_DIR = pathlib.Path(__file__).parent.parent / "data"

FIELDS_DATA    = json.loads((DATA_DIR / "fields.json").read_text())
SKILLS_DATA    = json.loads((DATA_DIR / "skills.json").read_text())
LOCATIONS_DATA = json.loads((DATA_DIR / "locations.json").read_text())

FIELD_SUPERSET = {f["value"]: f["superset"] for f in FIELDS_DATA["fields"]}
LOCATION_TO_COUNTRY = {loc["value"].lower(): loc["country"] for loc in LOCATIONS_DATA["locations"]}


REQUEST_DELAY   = 10
TIME_SECONDS    = 2160000  ## 60 Hours
MAX_JOBS_TOTAL  = 100_000  ## per scrape
BASE_DEPTH      = 250
MAX_DEPTH       = 5000



##helpers;

def desc_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a[:2000], b[:2000]).ratio()

### Fetch all data from table, and create sets and search_matrix
def fetch_all_dashboard_configs() -> list[dict]:
    resp = supabase.table("dashboard_configs").select(
        "id, user_id, include_skills, include_fields, include_locations, job_types"
    ).eq("active", True).execute()
    return resp.data or []

def build_sets(configs: list[dict]) -> tuple[set[str], set[str], set[str], set[str]]:
    skills: set[str]    = set()
    fields: set[str]    = set()
    locations: set[str] = set()
    job_types: set[str] = set()

    for cfg in configs:
        for s in (cfg.get("include_skills") or []):
            skills.add(s.lower().strip())
        for f in (cfg.get("include_fields") or []):
            fields.add(f.lower().strip())
        for loc in (cfg.get("include_locations") or []):
            locations.add(loc.lower().strip())
        for jt in (cfg.get("job_types") or []):
            job_types.add(jt.lower().strip())

    if not skills and not fields and not locations and not job_types:
        print("[scraper] incomplete configs — exiting")
        return set(), set(), set(), set()

    return skills, fields, locations, job_types


## go config by config, mapping importance of every search field by country to then fetch depth
def compute_votes(configs: list[dict]) -> dict[tuple[str, str], int]:
    votes: dict[tuple[str, str], int] = {}
    for cfg in configs:
        cfg_supersets = {FIELD_SUPERSET.get(f.lower().strip()) for f in (cfg.get("include_fields") or [])} - {None}
        cfg_countries = {LOCATION_TO_COUNTRY.get(loc.lower().strip()) for loc in (cfg.get("include_locations") or [])} - {None}
        for superset in cfg_supersets:
            for country in cfg_countries:
                votes[(superset, country)] = votes.get((superset, country), 0) + 1
    return votes


## will have to redesign depth logic in here though later!!
def get_depth(superset: str, country: str, votes: dict[tuple[str, str], int]) -> int:
    return min(BASE_DEPTH * max(1, votes.get((superset, country), 1)), MAX_DEPTH)


def build_search_matrix(fields: set[str], locations: set[str], job_types: set[str], votes: dict[tuple[str, str], int]) -> list[tuple[str, str, str]]:
    supersets: set[str] = set()
    countries: set[str] = set()

    for f in fields:
        superset = FIELD_SUPERSET.get(f)
        if superset:
            supersets.add(superset)
        else:
            print(f"[matrix] WARNING: field '{f}' not found in FIELD_SUPERSET — skipping")

    for loc in locations:
        country = LOCATION_TO_COUNTRY.get(loc)
        if country:
            countries.add(country)
        else:
            print(f"[matrix] WARNING: location '{loc}' not found in LOCATION_TO_COUNTRY — skipping")

    print(f"[matrix] mapped supersets: {supersets}")
    print(f"[matrix] mapped countries: {countries}")

    if not supersets or not countries:
        print("[scraper] couldn't map fields/locations to search matrix — exiting")
        return []

    matrix = [
        (f"{superset} {job_type}", country, job_type, get_depth(superset, country, votes))
        for superset in supersets
        for country in countries
        for job_type in job_types
    ]
    for entry in matrix:
        print(f"[matrix] entry → keyword='{entry[0]}' | country='{entry[1]}' | job_type='{entry[2]}' | depth={entry[3]}")
    return matrix
    
    
def _build_global_sets_skills_n_fields() -> tuple[set[str], set[str]]:
    all_fields: set[str] = set()
    all_skills: set[str] = set()

    for f in FIELDS_DATA["fields"]:
        all_fields.add(f["value"].lower())
    for s in FIELDS_DATA.get("supersets", []):
        all_fields.add(s.lower())

    def extract_strings(obj):
        if isinstance(obj, list):
            for item in obj:
                yield from extract_strings(item)
        elif isinstance(obj, dict):
            for v in obj.values():
                yield from extract_strings(v)
        elif isinstance(obj, str):
            yield obj.lower()

    all_skills = set(extract_strings(SKILLS_DATA))

    return all_fields, all_skills

GLOBAL_FIELDS, GLOBAL_SKILLS = _build_global_sets_skills_n_fields()
    
def match_jobs_to_configs(batch: list[dict], configs: list[dict]) -> list[dict]:
    user_jobs_insert_batch = []
    for job in batch:
        for config in configs:
            if apply_config_filters(job, config):
                user_jobs_insert_batch.append({
                    "user_id":             config["user_id"],
                    "dashboard_config_id": config["id"],
                    "job_id":              job["id"],  
                    "status":              "new",
                })
    return user_jobs_insert_batch
    
    
### layers 1-5;

def layer1_fields_in_title(fields: set[str], title_lower: str) -> bool:
    supersets = {FIELD_SUPERSET.get(f) for f in fields} - {None}
    
    for f in fields:
        if f in title_lower:
            return True
    for broad in supersets:
        if broad in title_lower:
            return True
    return False
    
    
def layer2_dedup(
    url: str,
    title: str,
    company: str,
    job_location: str,
    desc_text: str,
    url_set: set[str],
    title_company_map: dict[tuple[str, str], dict],
) -> tuple[str, dict | None]:
    if url in url_set:
        return "skip", None
    
    tc_key = (title.lower().strip(), company.lower().strip())
    if tc_key in title_company_map:
        for existing in title_company_map[tc_key]:
            sim = desc_similarity(desc_text, existing.get("description") or "")
            if sim > 0.85:
                existing_locs = existing.get("locations") or []
                if job_location in existing_locs:
                    return "replace", existing
                else:
                    return "merge", existing

        return "insert", None
    return "insert", None



def extract_salary(combined: str) -> dict | None:
    SALARY_PATTERNS = [
        (r'(?:\$|CAD|USD|C\$)\s*(\d[\d,]*(?:\.\d+)?)\s*(?:[-–]\s*(?:\$|CAD|USD|C\$)?\s*(\d[\d,]*(?:\.\d+)?))?\s*(?:\/\s*(?:hr|hour)|per\s+hour)', "hourly"),
        (r'(?:\$|CAD|USD|C\$)\s*(\d[\d,]*(?:\.\d+)?)\s*(?:[-–]\s*(?:\$|CAD|USD|C\$)?\s*(\d[\d,]*(?:\.\d+)?))?\s*(?:\/\s*(?:wk|week)|per\s+week)', "weekly"),
        (r'(?:\$|CAD|USD|C\$)\s*(\d[\d,]*(?:\.\d+)?)\s*(?:k|,000)?\s*(?:[-–]\s*(?:\$|CAD|USD|C\$)?\s*(\d[\d,]*(?:\.\d+)?)\s*(?:k|,000)?)?\s*(?:CAD|USD)?\s*(?:\/\s*(?:yr|year)|per\s+(?:year|annum)|annually)', "annual"),
        (r'(\d+)\s*(?:k|,000)\s*(?:[-–]\s*(\d+)\s*(?:k|,000)?)\s*(?:CAD|USD|per\s+year|annually)?', "annual"),
        (r'(?:salary|compensation|pay|rate|stipend|wage)[:\s]+(?:\$|CAD|USD|C\$)?\s*(\d[\d,]+)', "unknown"),
    ]
    
    matches = []
    seen = set()
    for pattern, pay_type in SALARY_PATTERNS:
        for m in re.finditer(pattern, combined, re.IGNORECASE):
            raw = m.group(0).strip()
            if raw not in seen:
                matches.append({"type": pay_type, "raw": raw})
                seen.add(raw)
    
    return {"matches": matches} if matches else None

    
def layer3_and_4_extract_metadata(
    title: str,
    desc_text: str,
    include_fields: set[str],
    include_skills: set[str],
) -> tuple[list[str], list[str], dict | None, dict | None, dict | None, dict | None, bool]:
    combined = (title + " " + desc_text).lower()

    matched_fields = [f for f in GLOBAL_FIELDS if re.search(rf'\b{re.escape(f)}\b', combined)]
    matched_skills = [s for s in GLOBAL_SKILLS if re.search(rf'\b{re.escape(s)}\b', combined)]

    salary = extract_salary(combined)

    SEASON_SIGNALS = [
        "spring", "summer", "fall", "autumn", "winter",
        "q1", "q2", "q3", "q4",
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
    ]
    SEASON_PATTERNS = [
        r'(spring|summer|fall|autumn|winter)\s*(?:\d{4})?',
        r'(q[1-4])\s+(\d{4})',
        r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})',
    ]
    season: dict | None = None
    found_season_signals = [s for s in SEASON_SIGNALS if s in combined]
    if found_season_signals:
        season_matches = []
        for pattern in SEASON_PATTERNS:
            for m in re.finditer(pattern, combined, re.IGNORECASE):
                season_matches.append(m.group(0).strip())
        season = {
            "signals": found_season_signals,
            "matches": list(dict.fromkeys(season_matches)),
        } if season_matches else {"signals": found_season_signals}


    DURATION_PATTERNS = [
        r'(\d+)\s*[-–]?\s*(month|months)',
        r'(\d+)\s*[-–]?\s*(week|weeks)',
        r'(\d+)\s*[-–]?\s*(year|years)',
        r'(full[- ]?year|year[- ]?long)',
        r'(permanent|contract|temporary|fixed[- ]?term)',
        r'(part[- ]?time|full[- ]?time)',
        r'(\d{4}[-/]\d{2}[-/]\d{2})\s*[-–to]+\s*(\d{4}[-/]\d{2}[-/]\d{2})',
        r'(\d{4}[-/]\d{2}[-/]\d{2})\s*[-–to]+\s*(\d{4})',
        r'(?:start(?:ing)?|begin(?:ning)?|commence[sd]?)[:\s]+(\w+ \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4})',
        r'(?:start(?:ing)?|begin(?:ning)?)[:\s]+(\w+\s+\d{4})',
        r'(\w+ \d{1,2}(?:st|nd|rd|th)?),?\s*(\d{4})\s*[-–to]+\s*(\w+ \d{1,2}(?:st|nd|rd|th)?),?\s*(\d{4})',
        r'(\d{1,2}\s+\w+\s+\d{4})\s*[-–to]+\s*(\d{1,2}\s+\w+\s+\d{4})'
    ]
    duration: dict | None = None
    duration_matches = []
    for pattern in DURATION_PATTERNS:
        for m in re.finditer(pattern, combined, re.IGNORECASE):
            duration_matches.append(m.group(0).strip())

    if duration_matches:
        duration = {"matches": list(dict.fromkeys(duration_matches))}


    REQ_SIGNALS = [
        "bachelor", "master", "phd", "degree", "diploma",
        "pursuing", "enrolled", "currently studying",
        "gpa", "1st year", "2nd year", "3rd year", "4th year",
        "final year", "penultimate", "recent graduate", "new grad",
        "years of experience", "year of experience",
        "no experience", "entry level", "junior",
    ]
    requirements: dict | None = None
    found_reqs = [sig for sig in REQ_SIGNALS if sig in combined]
    if found_reqs:
        windows = []
        for sig in found_reqs:
            idx = combined.find(sig)
            start = max(0, idx - 50)
            end = min(len(combined), idx + 300)
            windows.append((start, end))

        windows.sort()
        merged = [windows[0]]
        for start, end in windows[1:]:
            if start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))

        snippets = [combined[s:e] for s, e in merged]
        requirements = {"signals": found_reqs, "snippets": snippets}
        

    matched_fields_set = set(matched_fields)
    matched_skills_set = set(matched_skills)
    is_relevant = bool(
        matched_fields_set & include_fields or
        matched_skills_set & include_skills
    )

    return matched_fields, matched_skills, salary, duration, season, requirements, is_relevant


def layer5_insert_jobs(batch: list[dict]) -> tuple[int, list[dict]]:
    if not batch:
        return 0, []
    try:
        res = supabase.table("jobs").insert(batch).execute()
        return len(res.data), res.data  # res.data has the rows with IDs
    except Exception as e:
        print(f"[layer5] insert error: {e}")
        return 0, []


def layer5_replace(
    existing: dict,
    url: str,
    desc_text: str,
    matched_fields: list[str],
    matched_skills: list[str],
    salary: dict | None,
    duration: dict | None,
    season: dict | None,
    requirements: dict | None,
    url_set: set[str],
    title_company_map: dict,
    tc_key: tuple[str, str],
) -> None:
    try:
        supabase.table("jobs").update({
            "url":          url,
            "description":  desc_text,
            "fields":       matched_fields,
            "skills":       matched_skills,
            "salary":       salary,
            "duration":     duration,
            "season":       season,
            "requirements": requirements,
            "scraped_at":   datetime.now().isoformat(),
        }).eq("id", existing["id"]).execute()
        url_set.discard(existing.get("url"))
        url_set.add(url)
        title_company_map[tc_key]["url"] = url
        print(f"[layer5] REPLACE: {existing.get('title')} @ {existing.get('company')}")
    except Exception as e:
        print(f"[layer5] replace error: {e}")


def layer5_merge(
    existing: dict,
    job_location: str,
    url: str,
    url_set: set[str],
    title_company_map: dict,
    tc_key: tuple[str, str],
) -> None:
    try:
        existing_locs = existing.get("locations") or []
        merged_locs   = list(dict.fromkeys(existing_locs + [job_location]))
        supabase.table("jobs").update({
            "locations": merged_locs,
        }).eq("id", existing["id"]).execute()
        title_company_map[tc_key]["locations"] = merged_locs
        url_set.add(url)
        print(f"[layer5] MERGE: {existing.get('title')} @ {existing.get('company')} → {merged_locs}")
    except Exception as e:
        print(f"[layer5] merge error: {e}")


def fetch_job_detail(job_id: str) -> tuple[str, list[str]]:
    try:
        resp = requests.get(
            f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            timeout=15,
        )
        soup    = BeautifulSoup(resp.text, "html.parser")
        desc_el = soup.select_one("[class*=description] > section > div")
        tags_el = soup.select_one("[class*=_job-criteria-list]")
        desc    = desc_el.get_text(separator=" ", strip=True) if desc_el else ""
        tags    = [t.strip() for t in tags_el.get_text(separator="|").split("|") if t.strip()] if tags_el else []
        print(f"[detail] status={resp.status_code} | desc_len={len(desc)} | tags={tags}")
        return desc, tags
    except Exception as e:
        print(f"[detail] fetch error: {e}")
        return "", []



### MAIN RUN:
def run():
    print(f"[scraper] run started at {datetime.now().isoformat()}")

    configs = fetch_all_dashboard_configs()
    if not configs:
        print("[scraper] no active configs — exiting")
        return
    print(f"[scraper] {len(configs)} active dashboard config(s) loaded")
    for cfg in configs:
        print(f"  config id={cfg['id']} | fields={cfg.get('include_fields')} | locations={cfg.get('include_locations')} | job_types={cfg.get('job_types')} | skills={cfg.get('include_skills')}")

    skills, fields, locations, job_types = build_sets(configs)
    print(f"[sets] fields={fields} | locations={locations} | job_types={job_types} | skills={skills}")
    if not skills and not fields and not locations and not job_types:
        return

    votes         = compute_votes(configs)
    print(f"[votes] {votes}")
    search_matrix = build_search_matrix(fields, locations, job_types, votes)
    if not search_matrix:
        print("[scraper] empty search matrix — exiting")
        return
    print(f"[scraper] search matrix: {len(search_matrix)} entries")

    existing_resp = supabase.table("jobs").select("id, url, title, company, locations, description").execute()
    existing_rows = existing_resp.data or []
    url_set: set[str] = {r["url"] for r in existing_rows if r.get("url")}
    title_company_map: dict[tuple[str, str], list[dict]] = {}
    for r in existing_rows:
        key = (r["title"].lower().strip(), r["company"].lower().strip())
        title_company_map.setdefault(key, []).append(r)
    print(f"[scraper] {len(url_set)} existing jobs loaded into memory")

    ## --- scrape ---
    total_inserted  = 0
    trio_scraped: dict[tuple[str, str, str], int] = {}

    for keyword, country, job_type, depth_limit in search_matrix:
        if total_inserted >= MAX_JOBS_TOTAL:
            break

        trio_key = (keyword, country, job_type)
        print(f"\n[scraper] ── NEW SEARCH ──────────────────────────────────")
        print(f"[scraper] keyword='{keyword}' | country='{country}' | job_type='{job_type}' | depth={depth_limit}")

        start = 0
        batch: list[dict] = []
        page  = 0

        while True:
            if trio_scraped.get(trio_key, 0) >= depth_limit:
                print(f"[scraper] depth limit hit ({depth_limit}) — moving on")
                break
            if total_inserted >= MAX_JOBS_TOTAL:
                break

            print(f"[page] fetching page {page} (start={start}) for '{keyword}' in '{country}'")

            try:
                resp = requests.get(
                    "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search",
                    params={
                        "keywords": keyword,
                        "location": country,
                        "f_TPR":    f"r{TIME_SECONDS}",
                        "start":    start,
                    },
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                    timeout=15,
                )
                print(f"[page] response status={resp.status_code} | body_len={len(resp.text)}")
            except Exception as e:
                print(f"[scraper] request error: {e}")
                break

            soup  = BeautifulSoup(resp.text, "html.parser")
            cards = soup.find_all("div", class_="base-card")
            print(f"[page] found {len(cards)} cards on page {page}")
            if not cards:
                print(f"[page] no cards — stopping pagination")
                break

            cards_seen        = 0
            cards_no_title    = 0
            cards_no_url      = 0
            cards_layer1_fail = 0
            cards_dedup_skip  = 0
            cards_queued      = 0

            for card in cards:
                if trio_scraped.get(trio_key, 0) >= depth_limit:
                    break

                title_el = card.select_one("[class*=_title]")
                title    = title_el.get_text(strip=True) if title_el else None
                if not title:
                    cards_no_title += 1
                    continue

                url_el = card.select_one("[class*=_full-link]")
                url    = url_el["href"].split("?")[0] if url_el and url_el.get("href") else None
                if not url:
                    cards_no_url += 1
                    continue

                company_el  = card.select_one("[class*=_subtitle]")
                location_el = card.select_one("[class*=_location]")
                company      = company_el.get_text(strip=True)  if company_el  else "Unknown"
                job_location = location_el.get_text(strip=True) if location_el else country
                tc_key_card  = (title.lower().strip(), company.lower().strip())

                cards_seen += 1
                trio_scraped[trio_key] = trio_scraped.get(trio_key, 0) + 1

                print(f"  [card] title='{title}' | company='{company}' | loc='{job_location}'")

                ## layer 1
                passed_l1 = layer1_fields_in_title(fields, title.lower())
                if not passed_l1:
                    cards_layer1_fail += 1
                    print(f"  [layer1] FAIL — title='{title}'")
                    continue

                print(f"  [layer1] PASS — fetching detail...")
                job_id           = url.split("-")[-1]
                desc_text, _tags = fetch_job_detail(job_id)
                time.sleep(REQUEST_DELAY)

                ## layer 2
                action, existing = layer2_dedup(
                    url, title, company, job_location, desc_text, url_set, title_company_map
                )
                print(f"  [layer2] action={action}")

                if action == "skip":
                    cards_dedup_skip += 1
                    continue

                elif action == "replace":
                    matched_fields, matched_skills, salary, duration, season, requirements, is_relevant = layer3_and_4_extract_metadata(
                        title, desc_text, fields, skills
                    )
                    layer5_replace(existing, url, desc_text, matched_fields, matched_skills, salary, duration, season, requirements, url_set, title_company_map, tc_key_card)
                    continue

                elif action == "merge":
                    layer5_merge(existing, job_location, url, url_set, title_company_map, tc_key_card)
                    continue

                ## insert path
                matched_fields, matched_skills, salary, duration, season, requirements, is_relevant = layer3_and_4_extract_metadata(
                    title, desc_text, fields, skills
                )
                print(f"  [layer3+4] relevant={is_relevant} | matched_fields={matched_fields[:5]} | matched_skills={matched_skills[:5]}")

                if not is_relevant:
                    print(f"  [layer3+4] FAIL relevance — skipping")
                    continue

                item = {
                    "url":          url,
                    "title":        title,
                    "company":      company,
                    "location":     job_location,
                    "locations":    [job_location],
                    "description":  desc_text or None,
                    "source":       "linkedin",
                    "job_type":     job_type,
                    "fields":       matched_fields,
                    "skills":       matched_skills,
                    "salary":       salary,
                    "duration":     duration,
                    "season":       season,
                    "requirements": requirements,
                    "scraped_at":   datetime.now().isoformat(),
                }
                batch.append(item)
                cards_queued += 1
                url_set.add(url)
                title_company_map.setdefault(tc_key_card, []).append(item)
                print(f"  [queued] '{title}' @ '{company}' → batch size={len(batch)}")

            print(f"[page] summary — seen={cards_seen} | no_title={cards_no_title} | no_url={cards_no_url} | layer1_fail={cards_layer1_fail} | dedup_skip={cards_dedup_skip} | queued={cards_queued}")

            inserted, inserted_rows = layer5_insert_jobs(batch)
            total_inserted += inserted
            print(f"[layer5] inserted {inserted} jobs | total={total_inserted}")

            user_jobs_batch = match_jobs_to_configs(inserted_rows, configs)  # pass inserted_rows not batch
            if user_jobs_batch:
                for i in range(0, len(user_jobs_batch), 1000):
                    supabase.table("user_jobs").upsert(
                        user_jobs_batch[i:i+1000],
                        on_conflict="user_id,job_id,dashboard_config_id"
                    ).execute()
                print(f"[user_jobs] upserted {len(user_jobs_batch)} rows")
            batch.clear()

            start += 10
            page  += 1

        if batch:
            inserted       = layer5_insert_jobs(batch)
            total_inserted += inserted
            batch.clear()

    print(f"\n[scraper] finished at {datetime.now().isoformat()} — {total_inserted} new jobs inserted")


if __name__ == "__main__":
    run()