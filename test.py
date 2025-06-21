import os
import json
from datetime import datetime, timedelta
import re
from urllib.parse import urlparse

RECENT_WINDOW = {"days": 15}

def parse_relative_date(relative_date_str):
    if not relative_date_str:
        return None
    match = re.match(r'(\d+)([a-z]+)', relative_date_str)
    if not match:
        return None
    number, unit = match.groups()
    number = int(number)
    now = datetime.now()
    if unit in ['h', 'hr', 'hrs', 'hour', 'hours']:
        return now - timedelta(hours=number)
    elif unit in ['d', 'day', 'days']:
        return now - timedelta(days=number)
    elif unit in ['w', 'wk', 'wks', 'week', 'weeks']:
        return now - timedelta(weeks=number)
    elif unit in ['mo', 'month', 'months']:
        return now - timedelta(days=number * 30)
    elif unit in ['yr', 'year', 'years']:
        return now - timedelta(days=number * 365)
    return None

def is_recent(relative_date_str, window=None, default_months=6):
    date = parse_relative_date(relative_date_str)
    if not date:
        return False
    now = datetime.now()
    if window is None:
        window = {}
    if "days" in window:
        return date >= now - timedelta(days=window["days"])
    elif "months" in window:
        return date >= now - timedelta(days=window["months"] * 30)
    else:
        return date >= now - timedelta(days=default_months * 30)

def normalize_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/").lower()

def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.strip().lower()
    text = re.sub(r'\s+', ' ', text)
    return text

def deduplicate_activities(records):
    """
    Deduplicate by (post_text, comment, and post_url if present)
    """
    seen = set()
    unique = []
    for rec in records:
        post_text = normalize_text(rec.get("post_text", ""))
        comment = normalize_text(rec.get("comment", ""))
        post_url = normalize_url(rec.get("post_url", ""))
        key = (post_text, comment, post_url)
        if key not in seen:
            seen.add(key)
            unique.append(rec)
    return unique

def extract_social_activity_by_profile(act, current_company):
    """
    Build a mapping of engaged profile_url -> [activity list]
    """
    activity_map = {}
    # Collect by "other profile" (the person whose content was engaged)
    def add_activity(target_url, target_name, post_text, comment,source, engagement=None, timestamp=None ):
        if not target_url:
            return
        # Normalize
        target_url = normalize_url(target_url)
        post_text_norm = normalize_text(post_text)
        comment_norm = normalize_text(comment)
        # Build activity record
        record = {
            "post_author_name": target_name,
            "post_text": post_text,
            "comment": comment,
            "timestamp": timestamp,
            "source": [source],
            "engagement": engagement 
        }
        # Deduplication/merging: merge sources if same text+comment+url
        if target_url not in activity_map:
            activity_map[target_url] = []
        for existing in activity_map[target_url]:
            if (normalize_text(existing.get("post_text", "")) == post_text_norm and
                normalize_text(existing.get("comment", "")) == comment_norm):
                # Merge sources
                if source not in existing["source"]:
                    existing["source"].append(source)
                return
        activity_map[target_url].append(record)

    # POSTS
    for post in act.get("posts", []):
        if not is_recent(post.get("timestamp"), window=RECENT_WINDOW):
            continue
        if current_company and (post.get("author_name", "").lower() == current_company):
            continue
        post_text = post.get("text", "")
        if current_company and contains_company_name(post_text, current_company):
            continue
        add_activity(
            target_url=post.get("author_url"),
            target_name=post.get("author_name"),
            post_text=post_text,
            comment="",
            timestamp=post.get("timestamp"),
            source="post",
            engagement=post.get("engagement")
        )

    # COMMENTS
    for comment in act.get("comments", []):
        if not is_recent(comment.get("timestamp"), window=RECENT_WINDOW):
            continue
        if current_company and (comment.get("post_owner_name", "").lower() == current_company):
            continue
        comment_text = comment.get("text", "")
        parent_post_text = comment.get("parent_post_text", "")
        if current_company and (
            contains_company_name(comment_text, current_company) or 
            contains_company_name(parent_post_text, current_company)
        ):
            continue
        add_activity(
            target_url=comment.get("post_owner_url"),
            target_name=comment.get("post_owner_name"),
            post_text=parent_post_text,
            comment=comment_text,
            timestamp=comment.get("timestamp"),
            source="comment"
        )

    # REACTIONS
    for reaction in act.get("reactions", []):
        if not is_recent(reaction.get("timestamp"), window=RECENT_WINDOW):
            continue
        if current_company and (reaction.get("post_owner_name", "").lower() == current_company):
            continue
        post_text = reaction.get("post_text", "")
        if current_company and contains_company_name(post_text, current_company):
            continue
        add_activity(
            target_url=reaction.get("post_owner_url"),
            target_name=reaction.get("post_owner_name"),
            post_text=post_text,
            comment="",
            timestamp=reaction.get("timestamp"),
            source="reaction"
        )

    # Optionally, deduplicate per engaged profile
    for k in activity_map:
        activity_map[k] = deduplicate_activities(activity_map[k])
    return activity_map

def merge_social_activities(activity_list):
    """
    Merge activities by (post_text) regardless of comment.
    Output: [{post_author_name, post_text, actions: [{source, timestamp, comment}]}]
    """
    grouped = {}
    for act in activity_list:
        key = normalize_text(act["post_text"])
        if key not in grouped:
            grouped[key] = {
                "post_author_name": act["post_author_name"],
                "post_text": act["post_text"],
                "actions": [{
                    "source": [],
                    "post_timestamp": act.get("timestamp"),
                    "comment": None,
                    "engagement": act.get("engagement", {})
                }]
            }
        # Always use the same action slot
        action = grouped[key]["actions"][0]
        # Merge sources
        for s in (act["source"] if isinstance(act["source"], list) else [act["source"]]):
            if s not in action["source"]:
                action["source"].append(s)
        # Always prefer timestamp and comment if present from "comment" type
        if "comment" in action["source"]:
            if act.get("timestamp"):
                action["comment_timestamp"] = act.get("timestamp")
            if act.get("comment"):
                action["comment"] = act.get("comment")
                

        if act.get("engagement"):
            action["engagement"] = act["engagement"]
    # Clean up actions: remove 'comment' and 'timestamp' if not set
    for item in grouped.values():
        for action in item["actions"]:
            if not action.get("comment"):
                action.pop("comment", None)
            if not action.get("timestamp"):
                action.pop("timestamp", None)
            if not action.get("engagement"):
                action.pop("engagement", None)
    return list(grouped.values())

def contains_company_name(text, company_name):
    if not text or not company_name:
        return False
    pattern = r'\b' + re.escape(company_name) + r'\b'
    return bool(re.search(pattern, text, re.IGNORECASE))

def _current_company(basic, employment_history):
    if employment_history:
        return employment_history[0]["company"].lower()
    return None

def normalize_positions(positions):
    def parse_date_range(date_range):
        if not date_range:
            return None, None
        parts = date_range.split("·")[0].strip().split(" - ")
        start = parts[0].strip() if len(parts) > 0 else None
        end = parts[1].strip() if len(parts) > 1 else None
        return start, end
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

def normalize_profile(raw_data):
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
    current_company = _current_company(basic, employment_history)

    # --- The core: build activity map per engaged profile
    activity_map = extract_social_activity_by_profile(act, current_company)
    for profile_url in activity_map:
        activity_map[profile_url] = merge_social_activities(activity_map[profile_url])

    return {
        "basic_info": {
            "contact_id": basic.get("email", "").lower(),
            "full_name": basic.get("name"),
            "email": basic.get("email"),
            "location": basic.get("location"),
            "linkedin": basic.get("linkedin_profile_url"),
            "headline": basic.get("headline"),
            "position": {
                "title": employment_history[0]["positions"][0]["title"] if employment_history and employment_history[0]["positions"] else None,
                "company": employment_history[0]["company"] if employment_history else None,
                "location": employment_history[0]["positions"][0]["location"] if employment_history and employment_history[0]["positions"] else None,
            }
        },
        # "employment_history": employment_history,
        "social_activity": activity_map
    }

def normalize_folder(input_folder, output_folder):
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
