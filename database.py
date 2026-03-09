"""
Database module for Revenue Opportunity Scanner.
Stores opportunities, user profile insights, and completion tracking.
"""
import os
import ssl
import json
import logging
import asyncpg
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")


async def get_pool():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not set!")
    
    # Log masked URL for debugging
    masked = DATABASE_URL[:20] + "..." + DATABASE_URL[-20:] if len(DATABASE_URL) > 40 else "too_short"
    logger.info(f"Connecting to DB: {masked}")
    
    # Try with SSL first (Railway default), fallback without
    try:
        return await asyncpg.create_pool(
            DATABASE_URL, min_size=1, max_size=5, 
            ssl="require",
            timeout=10
        )
    except Exception as e1:
        logger.warning(f"SSL connection failed: {e1}, trying without SSL...")
        try:
            return await asyncpg.create_pool(
                DATABASE_URL, min_size=1, max_size=5,
                ssl=False,
                timeout=10
            )
        except Exception as e2:
            logger.warning(f"No-SSL failed too: {e2}, trying with ssl=prefer...")
            # Last attempt — let asyncpg figure it out
            return await asyncpg.create_pool(
                DATABASE_URL, min_size=1, max_size=5,
                timeout=10
            )


async def init_db(pool):
    """Create tables if they don't exist."""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS opportunities (
                id SERIAL PRIMARY KEY,
                project TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                action_items JSONB NOT NULL DEFAULT '[]',
                contact_person TEXT,
                contact_handle TEXT,
                potential_revenue TEXT,
                revenue_low INTEGER DEFAULT 0,
                revenue_high INTEGER DEFAULT 0,
                confidence TEXT DEFAULT 'medium',
                source_chat TEXT,
                source_date TIMESTAMPTZ,
                source_snippet TEXT,
                reasoning TEXT,
                status TEXT DEFAULT 'new',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                completed_at TIMESTAMPTZ,
                skipped_at TIMESTAMPTZ,
                skip_reason TEXT,
                priority INTEGER DEFAULT 5,
                tags TEXT[] DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS user_profile (
                id SERIAL PRIMARY KEY,
                profile_key TEXT UNIQUE NOT NULL,
                profile_value TEXT NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS scan_history (
                id SERIAL PRIMARY KEY,
                scan_type TEXT NOT NULL,
                chats_analyzed INTEGER DEFAULT 0,
                messages_analyzed INTEGER DEFAULT 0,
                opportunities_found INTEGER DEFAULT 0,
                started_at TIMESTAMPTZ DEFAULT NOW(),
                completed_at TIMESTAMPTZ,
                status TEXT DEFAULT 'running'
            );

            CREATE TABLE IF NOT EXISTS daily_plans (
                id SERIAL PRIMARY KEY,
                plan_date DATE DEFAULT CURRENT_DATE,
                plan_text TEXT NOT NULL,
                opportunity_ids INTEGER[] DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_opp_status ON opportunities(status);
            CREATE INDEX IF NOT EXISTS idx_opp_project ON opportunities(project);
            CREATE INDEX IF NOT EXISTS idx_opp_priority ON opportunities(priority);
        """)


def _parse_date(val):
    """Parse date string to datetime, or return None."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        from datetime import datetime as dt
        # Handle ISO format strings
        return dt.fromisoformat(val.replace("Z", "+00:00"))
    except Exception:
        return None


async def save_opportunity(pool, opp: dict) -> int:
    """Save a new opportunity and return its ID."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO opportunities 
                (project, title, description, action_items, contact_person, 
                 contact_handle, potential_revenue, revenue_low, revenue_high,
                 confidence, source_chat, source_date, source_snippet, 
                 reasoning, priority, tags)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            RETURNING id
        """,
            opp.get("project", "general"),
            opp["title"],
            opp["description"],
            json.dumps(opp.get("action_items", [])),
            opp.get("contact_person"),
            opp.get("contact_handle"),
            opp.get("potential_revenue", "unknown"),
            opp.get("revenue_low", 0),
            opp.get("revenue_high", 0),
            opp.get("confidence", "medium"),
            opp.get("source_chat"),
            _parse_date(opp.get("source_date")),
            opp.get("source_snippet"),
            opp.get("reasoning"),
            opp.get("priority", 5),
            opp.get("tags", []),
        )
        return row["id"]


async def get_active_opportunities(pool, limit=10, project=None):
    """Get active (new/in_progress) opportunities sorted by priority."""
    async with pool.acquire() as conn:
        if project:
            return await conn.fetch("""
                SELECT * FROM opportunities 
                WHERE status IN ('new', 'in_progress') AND project = $1
                ORDER BY priority ASC, revenue_high DESC, created_at ASC
                LIMIT $2
            """, project, limit)
        return await conn.fetch("""
            SELECT * FROM opportunities 
            WHERE status IN ('new', 'in_progress')
            ORDER BY priority ASC, revenue_high DESC, created_at ASC
            LIMIT $1
        """, limit)


async def mark_done(pool, opp_id: int):
    """Mark opportunity as completed."""
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE opportunities 
            SET status = 'done', completed_at = NOW()
            WHERE id = $1
        """, opp_id)


async def mark_skipped(pool, opp_id: int, reason: str = None):
    """Mark opportunity as skipped."""
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE opportunities 
            SET status = 'skipped', skipped_at = NOW(), skip_reason = $2
            WHERE id = $1
        """, opp_id, reason)


async def mark_in_progress(pool, opp_id: int):
    """Mark opportunity as in progress."""
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE opportunities SET status = 'in_progress' WHERE id = $1
        """, opp_id)


async def get_stats(pool):
    """Get summary statistics."""
    async with pool.acquire() as conn:
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) FILTER (WHERE status = 'new') as new_count,
                COUNT(*) FILTER (WHERE status = 'in_progress') as in_progress,
                COUNT(*) FILTER (WHERE status = 'done') as done_count,
                COUNT(*) FILTER (WHERE status = 'skipped') as skipped_count,
                COALESCE(SUM(revenue_low) FILTER (WHERE status = 'done'), 0) as revenue_realized_low,
                COALESCE(SUM(revenue_high) FILTER (WHERE status = 'done'), 0) as revenue_realized_high,
                COALESCE(SUM(revenue_low) FILTER (WHERE status IN ('new', 'in_progress')), 0) as revenue_pipeline_low,
                COALESCE(SUM(revenue_high) FILTER (WHERE status IN ('new', 'in_progress')), 0) as revenue_pipeline_high
            FROM opportunities
        """)
        return dict(stats)


async def get_opportunity_by_id(pool, opp_id: int):
    """Get a single opportunity by ID."""
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM opportunities WHERE id = $1", opp_id
        )


async def save_profile_insight(pool, key: str, value: str):
    """Save or update a user profile insight."""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO user_profile (profile_key, profile_value, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (profile_key) DO UPDATE 
            SET profile_value = $2, updated_at = NOW()
        """, key, value)


async def get_profile(pool) -> dict:
    """Get all profile insights."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT profile_key, profile_value FROM user_profile")
        return {row["profile_key"]: row["profile_value"] for row in rows}


async def save_scan(pool, scan_type: str) -> int:
    """Start a new scan record."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO scan_history (scan_type) VALUES ($1) RETURNING id
        """, scan_type)
        return row["id"]


async def complete_scan(pool, scan_id: int, chats: int, messages: int, opps: int):
    """Complete a scan record."""
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE scan_history 
            SET chats_analyzed = $2, messages_analyzed = $3, 
                opportunities_found = $4, completed_at = NOW(), status = 'done'
            WHERE id = $1
        """, scan_id, chats, messages, opps)


async def check_duplicate(pool, title: str, source_chat: str) -> bool:
    """Check if a similar opportunity already exists."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT id FROM opportunities 
            WHERE title = $1 AND source_chat = $2 AND status != 'skipped'
        """, title, source_chat)
        return row is not None
