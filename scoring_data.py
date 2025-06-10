from datetime import datetime, timedelta
import re

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
    elif unit in ['w', 'wk', 'week', 'weeks']:
        return now - timedelta(weeks=number)
    elif unit in ['mo', 'month', 'months']:
        return now - timedelta(days=number * 30)
    elif unit in ['yr', 'year', 'years']:
        return now - timedelta(days=number * 365)
    return None

def recency_factor(date_str):
    dt = parse_relative_date(date_str)
    if not dt:
        return 0.5  # treat as old
    days_ago = (datetime.now() - dt).days
    if days_ago <= 7:
        return 2
    elif days_ago <= 30:
        return 1.5
    elif days_ago <= 90:
        return 1
    else:
        return 0.5

def engagement_score(data):
    # Type weights
    type_weights = {
        'recent_posts': 3,
        'recent_comments': 2,
        'reactions_given': 1
    }
    total_score = 0
    details = []

    for interaction_type in ['recent_posts', 'recent_comments', 'reactions_given']:
        interactions = data.get('social_activity', {}).get(interaction_type, [])
        for item in interactions:
            # Get the date string key
            date_str = item.get('timestamp')
            base = type_weights[interaction_type]
            recency = recency_factor(date_str)
            score = base * recency
            total_score += score
            details.append({
                "type": interaction_type,
                "timestamp": date_str,
                "score": score,
                "text": item.get("text") or item.get("post_text_snippet", "")
            })
    return {
        "contact_id": data.get("contact_id"),
        "engagement_score": round(total_score, 2),
        "details": details
    }

# Usage Example:
import json
with open('Normalize_data/Scoring.json', 'r', encoding='utf-8') as f:
    profile = json.load(f)
result = engagement_score(profile)
print(json.dumps(result, indent=2))
