import aiomysql
import os
import re
from dotenv import load_dotenv
import logging

load_dotenv()

# ── Connection config ─────────────────────────────────────────────────────────
# Reads the same env vars your other project uses:
#   DB_Url=jdbc:mysql://1.1.12.1:3306/?useUnicode=true&...
#   DB_Username=...
#   DB_Password=...

_DB_URL      = os.getenv("DB_Url", "")
_DB_USERNAME = os.getenv("DB_Username")
_DB_PASSWORD = os.getenv("DB_Password")


def _parse_host_port(jdbc_url: str) -> tuple[str, int]:
    """Extract host and port from a jdbc:mysql://host:port/... URL."""
    match = re.search(r"mysql://([^:/]+):?(\d+)?/", jdbc_url)
    if not match:
        raise ValueError(f"Cannot parse DB_Url: {jdbc_url!r}")
    host = match.group(1)
    port = int(match.group(2)) if match.group(2) else 3306
    return host, port


_DB_HOST, _DB_PORT = _parse_host_port(_DB_URL) if _DB_URL else ("localhost", 3306)

# ── Per-database pool cache ───────────────────────────────────────────────────
# One pool per repository database  { db_name: aiomysql.Pool }
_pools: dict[str, aiomysql.Pool] = {}


def repo_to_db_name(repository_id: str) -> str:
    return f"{repository_id}_EEATool"


async def get_pool(repository_id: str) -> aiomysql.Pool:
    """Return (or lazily create) a connection pool for the given repository's database."""
    db_name = repo_to_db_name(repository_id)

    if db_name not in _pools:
        _pools[db_name] = await aiomysql.create_pool(
            host=_DB_HOST,
            port=_DB_PORT,
            user=_DB_USERNAME,
            password=_DB_PASSWORD,
            db=db_name,
            autocommit=True,
            minsize=2,
            maxsize=20,
            charset="utf8mb4",
        )
        logging.info(f"✅ MySQL pool created for database: {db_name}")

    return _pools[db_name]


async def close_all_pools():
    """Close every open pool — call on app shutdown."""
    for db_name, pool in _pools.items():
        pool.close()
        await pool.wait_closed()
        logging.info(f"🔒 Closed pool for database: {db_name}")
    _pools.clear()


# ── Table DDL ─────────────────────────────────────────────────────────────────
_CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS private_messages (
        id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        sender_id     int(11) NOT NULL,
        receiver_id   int(11) NOT NULL,
        content       TEXT,
        file_name     VARCHAR(500)  DEFAULT NULL,
        file_url      VARCHAR(1000) DEFAULT NULL,
        timestamp     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_pm_sender   (sender_id),
        INDEX idx_pm_receiver (receiver_id),
        INDEX idx_pm_conv     (sender_id, receiver_id),
        FOREIGN KEY (sender_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (receiver_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS messenger_groups (
        id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        name        VARCHAR(255) NOT NULL UNIQUE,
        creator_id  int(11) NOT NULL,
        group_type  ENUM('normal','broadcast') NOT NULL DEFAULT 'normal',
        created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_grp_creator (creator_id),
        FOREIGN KEY (creator_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS group_members (
        group_id    BIGINT UNSIGNED NOT NULL,
        member_id   int(11)    NOT NULL,
        PRIMARY KEY (group_id, member_id),
        FOREIGN KEY (group_id) REFERENCES messenger_groups(id) ON DELETE CASCADE,
        FOREIGN KEY (member_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS group_messages (
        id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        sender_id   int(11)    NOT NULL,
        group_id    BIGINT UNSIGNED NOT NULL,
        content     TEXT,
        file_name   VARCHAR(500)    DEFAULT NULL,
        file_url    VARCHAR(1000)   DEFAULT NULL,
        timestamp   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_gm_group  (group_id),
        INDEX idx_gm_sender (sender_id),
        FOREIGN KEY (sender_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (group_id) REFERENCES messenger_groups(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]

# Which databases have already had their tables verified this session
_initialised_dbs: set[str] = set()


async def init_db(repository_id: str):
    """
    Ensure messenger tables exist in this repository's database.
    No-ops on subsequent calls for the same repository this session.
    """
    db_name = repo_to_db_name(repository_id)
    if db_name in _initialised_dbs:
        return

    pool = await get_pool(repository_id)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for sql in _CREATE_TABLES_SQL:
                await cur.execute(sql)

    _initialised_dbs.add(db_name)
    logging.info(f"✅ Tables verified/created in: {db_name}")