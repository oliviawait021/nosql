# MongoDB Social Media Data Platform

This project migrates a social media platform's data layer from a relational database to MongoDB, implementing both transactional application logic and an analytics extraction pipeline. I built the full data lifecycle: ingesting heterogeneous post and comment data, handling schema evolution without migrations, and exporting analytics-ready Parquet files partitioned by date.

## Tech Stack

- **MongoDB 6.0** (containerized via Docker)
- **Python 3.14** · **PyMongo** · **pandas** · **PyArrow**
- **Docker / Docker Compose**
- **uv** (Python package manager)

## Key Features

- **Dual-schema document design** — initial posts store likes as a `user_likes` array; newer posts use a numeric `like_count`. Both coexist in the same collection and are handled transparently at query time via aggregation pipelines.
- **Embedded comments** — comments are denormalized into post documents, eliminating join overhead for the most common read pattern.
- **Idempotent like tracking** — `$addToSet` prevents duplicate likes on old posts; `$inc` increments the counter on new posts. User engagement is mirrored on the user document via `posts_liked`.
- **Full-text search** — text index on the `text` field with `$text` operator queries.
- **Batch analytics export** — extracts and normalizes all posts into a consistent schema and writes partitioned Parquet files (`out/posts_analytics/post_date=YYYY-MM-DD/`), with partition-level overwrites for idempotent re-runs.

## What I Learned

- Designing for schema flexibility instead of enforcing rigid structure upfront
- Using `$cond` and `$ifNull` in aggregation pipelines to unify heterogeneous document shapes at query time
- The tradeoffs between document embedding (fast reads, harder updates) and referencing (normalized, join-heavy)
- Building idempotent batch extraction pipelines that are safe to re-run

## Running Locally

**Prerequisites:** Docker, Python 3.14+, `uv`

```bash
# Start MongoDB
docker compose up -d

# Install dependencies
uv sync

# Load data (import users.json via MongoDB Compass first)
uv run python insert_initial_posts.py
uv run python insert_new_posts.py

# Run application logic
uv run python read_functions.py
uv run python like_post.py
uv run python find_with_index.py

# Export analytics Parquet files
uv run python extract_posts_analytics.py
# Output: out/posts_analytics/post_date=YYYY-MM-DD/posts.parquet
```

MongoDB connection: `mongodb://root:password@localhost:27017`
