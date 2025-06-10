import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any
import re
from collections import defaultdict
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("linkedin_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_relative_date(relative_date_str):
    if not relative_date_str:
        return None
    match = re.match(r'(\d+)([a-z]+)', relative_date_str)
    if not match:
        return None

    number, unit = match.groups()
    number = int(number)
    now = datetime.now()

    if unit in ['d', 'day', 'days']:
        return now - timedelta(days=number)
    elif unit in ['mo', 'month', 'months']:
        return now - timedelta(days=number * 30)
    elif unit in ['yr', 'year', 'years']:
        return now - timedelta(days=number * 365)
    return None


def is_recent(relative_date_str, months=6):
    date = parse_relative_date(relative_date_str)
    return date and date >= datetime.now() - timedelta(days=months * 30)


def normalize_date(date_str: str):
    if not date_str or 'present' in date_str.lower():
        return None
    try:
        return datetime.strptime(date_str, "%b %Y").strftime("%Y-%m")
    except ValueError:
        return None


def _current_company(basic: Dict[str, Any], employment_history: list[Dict[str, Any]]) -> str | None:
    """
    Return a single lowercase string representing the user’s present company.
    Priority:
      1) basic['headline'] text after an “@”
      2) first employment_history item (assumed latest company)
    """
    # headline = basic.get("headline", "")
    # if "@" in headline:
    #     return headline.split("@")[-1].strip().lower()

    if employment_history:
        return employment_history[0]["company"].lower()

    return None

def parse_date_range(date_range: str):
    if not date_range:
        return None, None
    parts = date_range.split("·")[0].strip().split(" - ")
    start = parts[0].strip() if len(parts) > 0 else None
    end = parts[1].strip() if len(parts) > 1 else None
    return normalize_date(start), normalize_date(end)


def normalize_positions(positions):
    return [
        {
            "title": pos.get("title"),
            "start_date": parse_date_range(pos.get("date_range"))[0],
            "end_date": parse_date_range(pos.get("date_range"))[1],
            "location": pos.get("location"),
            "description": pos.get("description")
        }
        for pos in positions
    ]

def normalize_profile(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    basic   = raw_data.get("basic_info", {})
    exp     = raw_data.get("experience", {})
    act     = raw_data.get("activity", {})

    employment_history = [
        {
            "company"    : company,
            "company_url": details.get("company_url"),
            "positions"  : normalize_positions(details.get("positions", []))
        }
        for company, details in exp.items()
    ]

    latest_position = (
        employment_history[0]["positions"][0]
        if employment_history and employment_history[0]["positions"]
        else {}
    )

    current_company = _current_company(basic, employment_history)
    

    posts, seen_post_snips = [], set()
    for post in act.get("posts", []):
        if not is_recent(post.get("timestamp")):
            continue
        if current_company and (post.get("author_name", "").lower() == current_company):
            continue                                
        snippet = post.get("text", "")
        if snippet in seen_post_snips:
            continue
        seen_post_snips.add(snippet)

        posts.append({
            "post_author": post.get("author_name"),
            "author_url":post.get("author_url"),
            "text": snippet,
            "likes": int(post.get("engagement", {}).get("likes", 0)),
            "comments": int(post.get("engagement", {}).get("comments", 0)),
            "shares": int(post.get("engagement", {}).get("shares", 0)),
            "timestamp": post.get("timestamp"),
            "reposted": post.get("reposted")
        })

    comments, seen_comment_keys = [], set()
    for comment in act.get("comments", []):
        if not is_recent(comment.get("timestamp")):
            continue
        if current_company and (comment.get("post_owner_name", "").lower() == current_company):
            continue
        key = comment.get("post_url") or comment.get("text", "")
        if key in seen_comment_keys:
            continue
        seen_comment_keys.add(key)

        comments.append({
            "post_owner": comment.get("post_owner_name"),
            "post_owner_url": comment.get("post_owner_url"),
            "post_url": comment.get("post_url"),
            "post_text": comment.get("parent_post_text", ""),
            "comment": comment.get("text", ""),
            "timestamp": comment.get("timestamp")
        })

    reactions_given, seen_snippets = [], set()
    for reaction in act.get("reactions", []):
        if not is_recent(reaction.get("timestamp")):
            continue
        if current_company and (reaction.get("post_owner_name", "").lower() == current_company):
            continue
        snippet = reaction.get("post_text", "")
        if snippet in seen_snippets:
            continue
        seen_snippets.add(snippet)

        reactions_given.append({
            "post_owner": reaction.get("post_owner_name"),
            "post_owner_url": reaction.get("post_owner_url"),
            "post_url": reaction.get("post_url"),
            "post_text_snippet": snippet,
            "timestamp": reaction.get("timestamp")
        })
    return {
        "basic_info":{
        "contact_id": basic.get("email", "").lower(),
        "full_name": basic.get("name"),
        "email": basic.get("email"),
        "location": basic.get("location"),
        "linkedin": basic.get("linkedin_profile_url"),
        "headline": basic.get("headline"),
        "current_position": {
            "title": latest_position.get("title"),
            "company": employment_history[0]["company"] if employment_history else None,
            "location": latest_position.get("location")
        }
        },
        "employment_history": employment_history,
        "social_activity": {
            "recent_posts": posts,
            "recent_comments": comments,
            "reactions_given": reactions_given
        },
        "metadata": {
            "scraped_at": raw_data.get("scraped_at"),
            "source_profile": raw_data.get("profile_url")
        }
    }



def normalize_folder(input_folder: str, output_folder: str):
    os.makedirs(output_folder, exist_ok=True)
    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            input_path = os.path.join(input_folder, filename)
            output_path = os.path.join(output_folder, filename)

            with open(input_path, "r", encoding="utf-8") as infile:
                raw_data = json.load(infile)

            normalized = normalize_profile(raw_data)
     
            with open(output_path, "w", encoding="utf-8") as outfile:
                json.dump(normalized, outfile, indent=2)

            print(f"✔ Processed {filename}")


if __name__ == "__main__":
    normalize_folder("profiles_scraped", "Normalize_data")
