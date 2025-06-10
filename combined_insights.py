import os
import json
import re
from collections import defaultdict
from datetime import datetime
from sentence_transformers import SentenceTransformer, util
from data_normalizer import normalize_profile

# Load intent classification model and phrase embeddings
model = SentenceTransformer("all-MiniLM-L6-v2")

# Define intent labels and their associated phrases
INTENT_LABELS = {
    "job_change": [
        "new role", "starting a new job", "excited to join", "promotion", "promoted to",
        "career move", "beginning a new position", "role change", "COO", "VP", "Director", "new opportunity"
    ],
    "ai_interest": [
        "AI tools", "exploring AI", "ChatGPT", "artificial intelligence", "AI platform",
        "machine learning", "AI-powered", "AI buyers", "AI compliance", "data privacy", "automation"
    ],
    "marketing_automation": [
        "marketing automation", "automation platform", "Marketo", "HubSpot workflow",
        "campaign automation", "MarTech", "demand gen", "programmatic nurture", "lead nurture"
    ],
    "vendor_research": [
        "comparing", "vendor evaluation", "Researching", "platform evaluation", "demo request", "RFP"
    ],
    "team_expansion": [
        "we're hiring", "expanding the team", "open roles", "now hiring", "hiring for"
    ],
    "product_launch": [
        "launched", "new product", "product release", "introducing", "feature release"
    ],
    "thought_leadership": [
        "webinar", "thought leadership", "keynote", "panelist", "speaking at", "conference"
    ],
    "gratitude_celebration": [
        "thank you", "grateful", "appreciate", "congrats", "celebrating", "milestone"
    ],
    # Add more categories as needed
}

# Precompute phrase embeddings
_all_phrases = []
_phrase_to_label = []
for label, phrases in INTENT_LABELS.items():
    for phrase in phrases:
        _all_phrases.append(phrase)
        _phrase_to_label.append(label)
_PHRASE_EMBEDDINGS = model.encode(_all_phrases, normalize_embeddings=True)


def normalize_text(text: str) -> str:
    """
    Lowercases, removes URLs and extra whitespace.
    """
    t = text.lower()
    t = re.sub(r'http\S+', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def classify_intents(texts, threshold=0.5):
    """
    Returns a list of intent labels whose similarity
    with any text exceeds the threshold.
    """
    if not texts:
        return []
    embeddings = model.encode(texts, normalize_embeddings=True)
    sims = util.cos_sim(embeddings, _PHRASE_EMBEDDINGS).numpy()
    labels = set()
    for row in sims:
        for idx, score in enumerate(row):
            if score > threshold:
                labels.add(_phrase_to_label[idx])
    return list(labels)


def extract_intent_signals_from_profile(profile_data: dict, threshold=0.5) -> list:
    """
    Gathers all text snippets from recent posts, comments, and reactions,
    classifies them, and returns detected intent labels.
    """
    snippets = []
    for post in profile_data.get("social_activity", {}).get("recent_posts", []):
        if text := post.get("text"): snippets.append(normalize_text(text))
    for comment in profile_data.get("social_activity", {}).get("recent_comments", []):
        if text := comment.get("text"): snippets.append(normalize_text(text))
    for reaction in profile_data.get("social_activity", {}).get("reactions_given", []):
        if snippet := reaction.get("post_text_snippet"): snippets.append(normalize_text(snippet))
    return classify_intents(snippets, threshold)

def get_company_engagement_counts(profile_data, name_to_company_map=None):
    def resolve_company(name):
        return name_to_company_map.get(name, name) if name_to_company_map else name

    # 1) Grab the basic-info block if it exists, else use the top‐level
    bi = profile_data.get("basic_info") or profile_data

    contact_info = {
        "contact_id": bi.get("contact_id"),
        "full_name": bi.get("full_name"),
        "email": bi.get("email"),
        "location": bi.get("location"),
        "linkedin": bi.get("linkedin"),
        "headline": bi.get("headline"),
        # if your schema uses "current_position" at the top‐level or under basic_info:
        "current_position": bi.get("current_position", profile_data.get("current_position", {})),
    }

    # 2) Build per‐company counters
    counts = defaultdict(lambda: {
        "posts": 0,
        "comments": 0,
        "reactions": 0,
        "total": 0,
        "urls": set()
    })

    contact_name = contact_info["full_name"]
    sa = profile_data.get("social_activity", {})

    # a) Reposts of others’ content
    for post in sa.get("recent_posts", []):
        author = post.get("post_author", "").strip()
        if author and author != contact_name and post.get("reposted") == 1:
            comp = resolve_company(author)
            counts[comp]["posts"] += 1
            counts[comp]["total"] += 1
            url = post.get("author_url")
            if url:
                counts[comp]["urls"].add(url)

    # b) Comments made by the contact
    for comment in sa.get("recent_comments", []):
        owner = comment.get("post_owner", "").strip()
        if owner:
            comp = resolve_company(owner)
            counts[comp]["comments"] += 1
            counts[comp]["total"] += 1
            url = comment.get("post_owner_url")
            if url:
                counts[comp]["urls"].add(url)

    # c) Reactions given by the contact
    for react in sa.get("reactions_given", []):
        owner = react.get("post_owner", "").strip()
        if owner:
            comp = resolve_company(owner)
            counts[comp]["reactions"] += 1
            counts[comp]["total"] += 1
            url = react.get("post_owner_url")
            if url:
                counts[comp]["urls"].add(url)

    # 3) Convert each URL set into a list
    engagement = {}
    for comp, stats in counts.items():
        if stats["total"] >= 3:
            stats["urls"] = list(stats["urls"])
            engagement[comp] = stats
       

    return {
        "contact": contact_info,
        "engagement": engagement
    }


if __name__ == "__main__":
    # Path to raw scraped JSON
    raw_path = os.path.join("profiles_scraped", "test_1.json")
    with open(raw_path, encoding="utf-8") as f:
        raw_data = json.load(f)

    # Normalize raw data
    normalized = normalize_profile(raw_data)

    #engagement summary
    engagement = get_company_engagement_counts(normalized)

    # Extract intent signals
    intents = extract_intent_signals_from_profile(normalized, threshold=0.6)

    # Combine results
    result = {
        "summary": engagement,
        "intent_signals": intents
    }
    
    # Output combined JSON
    os.makedirs("insights", exist_ok=True)
    out_path = os.path.join("insights", "combined_insights.json")
    with open(out_path, "w", encoding="utf-8") as out:
        json.dump(result, out, indent=2)
