import re
from collections import defaultdict
import spacy
from sentence_transformers import SentenceTransformer, util
import json
import os 
from dotenv import load_dotenv
from openai import OpenAI
import openai
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


client = OpenAI()
load_dotenv()
# Load models once
nlp = spacy.load("en_core_web_lg")
model = SentenceTransformer("all-MiniLM-L6-v2")

# Multi-phrase intent definitions
INTENT_LABELS = {
    "job_change": [
        "new role", "starting a new job", "excited to join", "promotion", "promoted to", "career move", 
        "beginning a new position", "role change", "COO", "VP", "Director", "new opportunity"
    ],
    "ai_interest": [
        "AI tools", "exploring AI", "ChatGPT", "artificial intelligence", "AI platform", "machine learning", 
        "AI-powered", "AI buyers", "AI compliance", "data privacy", "ai", "automation", "AI conference", 
        "future of ai", "ai for business"
    ],
    "marketing_automation": [
        "marketing automation", "automation platform", "Marketo", "HubSpot workflow", "marketing technology",
        "campaign automation", "MarTech", "demand gen", "programmatic nurture", "lead nurture", "B2B marketing", 
        "ABM", "marketing operations", "lead generation", "pipeline", "customer journey"
    ],
    "vendor_research": [
        "comparing", "vendor evaluation", "looking at", "CRM vendors", "Salesforce", "HubSpot", "Marketo", "Zoho", 
        "researching", "platform evaluation", "trying out", "switching to", "assessment", "platform selection",
        "demo request", "RFP", "trial"
    ],
    "team_expansion": [
        "we're hiring", "expanding the team", "join our team", "open roles", "expanding workforce", "growing our team",
        "hiring for", "now hiring", "talent acquisition", "adding headcount"
    ],
    "product_launch": [
        "launched", "new product", "product release", "introducing", "just launched", "now available", 
        "product update", "major update", "feature release", "launching soon", "new feature"
    ],
    "thought_leadership": [
        "webinar", "thought leadership", "keynote", "insightful session", "panelist", "speaking at", "conference", 
        "guest speaker", "sharing thoughts", "trends", "future predictions", "report", "whitepaper"
    ],
    "gratitude_celebration": [
        "thank you", "grateful", "appreciate", "proud", "shoutout", "congrats", "celebrating", "milestone", "achievement"
    ],
    "customer_success": [
        "customer success", "client win", "client story", "customer journey", "case study", "customer testimonial",
        "client retention"
    ],
    "leadership_growth": [
        "leadership", "operational excellence", "driving growth", "scalable success", "team leadership", 
        "organizational growth", "expansion", "executive team", "management", "building teams"
    ]
}

# Precompute phrase embeddings
all_phrases = []
phrase_label_map = []
for label, phrases in INTENT_LABELS.items():
    for p in phrases:
        all_phrases.append(p)
        phrase_label_map.append(label)
PHRASE_EMBEDDINGS = model.encode(all_phrases, normalize_embeddings=True)

# ─── Helper Functions ──────────────────────────────────────────────────────────
def normalize_text(text: str) -> str:
    text = text.lower()
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'\S+@\S+', '', text)
    return re.sub(r'\s+', ' ', text).strip()

def classify_intents(texts: list[str], threshold: float = 0.5) -> list[str]:
    if not texts:
        return []
    text_embeddings = model.encode(texts, batch_size=32, normalize_embeddings=True)
    sim = util.cos_sim(text_embeddings, PHRASE_EMBEDDINGS).numpy()
    labels = set()
    for row in sim:
        for idx, score in enumerate(row):
            if score > threshold:
                labels.add(phrase_label_map[idx])
    return list(labels)

def extract_company_mentions(texts: list[str], min_count: int = 2) -> dict:
    org_counter = defaultdict(int)
    org_variants = defaultdict(set)

    for doc in nlp.pipe(texts, batch_size=64):
        for ent in doc.ents:
            if ent.label_ == "ORG":
                norm = ent.text.strip().lower()
                if len(norm) > 2 and norm not in {"team", "group", "company", "department"}:
                    org_counter[norm] += 1
                    org_variants[norm].add(ent.text.strip())

    high_interest = {k: v for k, v in org_counter.items() if v >= min_count}
    return {
        "all_mentions": dict(org_counter),
        "high_interest_companies": high_interest,
        "original_variants": org_variants
    }

def filter_real_companies(all_mentions: dict[str,int],
                          variants: dict[str,set[str]]) -> dict[str,int]:
    real = {}
    for norm, count in all_mentions.items():
        # pick one original-cased variant for NER check
        sample = next(iter(variants[norm]))
        doc = nlp(sample)
        if any(ent.label_ == "ORG" for ent in doc.ents):
            real[norm] = count
    return real

# ─── Main Analysis Function ───────────────────────────────────────────────────
def analyze_intent_and_companies(raw: dict) -> dict:
    sa = raw.get("social_activity", {})

    posts = [p.get("text", "") for p in sa.get("recent_posts", []) if p.get("text")]
    comments = [c.get("text", "") for c in sa.get("recent_comments", []) if c.get("text")]
    reactions = [
        r.get("post_text_snippet", "")
        for r in sa.get("reactions_given", [])
        if r.get("post_text_snippet")
    ]

    # allow even short mentions through
    texts = [
        normalize_text(t)
        for t in posts + comments + reactions
        if t and t.strip()
    ]

    intent_signals = classify_intents(texts)
    companies = extract_company_mentions(texts)
    filtered = filter_real_companies(companies["all_mentions"], companies["original_variants"])
    high_interest = {
        k: v
        for k, v in companies["high_interest_companies"].items()
        if k in filtered
    }

    return {
        "intent_signals": intent_signals,
        "company_mentions": filtered,
        "high_interest_companies": high_interest
    }

# ─── Script Entry Point ───────────────────────────────────────────────────────
if __name__ == "__main__":
    with open("Normalize_data/Scoring.json", "r", encoding="utf-8") as f:
        profile_json = json.load(f)

    insights = analyze_intent_and_companies(profile_json)

    os.makedirs("text_insights", exist_ok=True)
    with open("text_insights/insights_data.json", "w", encoding="utf-8") as out:
        json.dump(insights, out, indent=2)

    logger.info("Analysis complete, results written to text_insights/insights_data.json")