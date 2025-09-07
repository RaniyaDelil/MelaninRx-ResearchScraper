# Black Maternal Health Reddit Scraper

This project collects a **small, focused dataset** of Reddit posts related to **Black women's pregnancy and healthcare experiences**.  
It uses the official [Reddit API](https://www.reddit.com/dev/api/) (via [PRAW](https://praw.readthedocs.io/)) to stay compliant with Reddit’s Terms of Service.

The scraper targets:
- **Black-focused subreddits** (e.g., r/BlackWomen, r/BlackLadies) → where Black identity is implied.
- **General pregnancy/health subreddits** (e.g., r/BabyBumps, r/pregnant, r/AskWomen) → but only keeps posts that mention Black identity **and** pregnancy/health.

⚠️ **Important**: This repository contains only **sanitized code and sample outputs**. Raw Reddit data is never committed.

---

## Features
- Collects posts from multiple subreddits with keyword search.
- Filters for pregnancy/health + Black identity mentions.
- Hashes author usernames for privacy.
- Saves results in JSONL (`data_raw/reddit_posts.jsonl`).

---

## Setup

### 1. Clone and enter repo
```bash
git clone https://github.com/yourusername/MelaninRxScraper.git
cd MelaninRxScraper
```

### 2. Create virtual environment
```bash
python -m venv .venv
# activate (Git Bash)
source .venv/Scripts/activate
# or PowerShell
.venv\Scripts\Activate.ps1
```

### 3. Install requirements
```bash
pip install -r requirements.txt
```

### 4. Set up Reddit API credentials
1. Log in at https://www.reddit.com/prefs/apps
2. Create a new script app.
    * Name: MelaninRx_health_research
    * Redirect URI: http://localhost:8080
    * Copy the client_id and client_secret.

3. Create a .env file in the repo root:
    * REDDIT_CLIENT_ID=your_client_id
    * REDDIT_CLIENT_SECRET=your_client_secret
    * REDDIT_USER_AGENT=MelaninRx_health_research:v0.1 (by /u/your_username)

 Never commit your .env, it’s in .gitignore.

### 5. Usage
Run the scraper:
```bash
python reddit_ingest.py
```

View results:
```bash
head -n 5 data_raw/reddit_posts.jsonl
```
