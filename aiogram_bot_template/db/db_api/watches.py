import asyncpg
from typing import List, Dict, Any

class WatchesRepo:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def add_watch(self, user_id: int, original_url: str, api_url: str, last_car_seq: str | None) -> int:
        sql = """
        INSERT INTO kb_watches (user_id, original_url, api_url, last_car_seq)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, user_id, original_url, api_url, last_car_seq)

    async def get_active_watches(self) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM kb_watches WHERE is_active = TRUE"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql)
            return [dict(row) for row in rows]

    async def update_last_car_seq(self, watch_id: int, new_seq: str):
        sql = "UPDATE kb_watches SET last_car_seq = $1 WHERE id = $2"
        async with self.pool.acquire() as conn:
            await conn.execute(sql, new_seq, watch_id)
            
    async def deactivate_watch(self, watch_id: int):
        sql = "UPDATE kb_watches SET is_active = FALSE WHERE id = $1"
        async with self.pool.acquire() as conn:
            await conn.execute(sql, watch_id)

    async def get_watchers_to_check(self) -> List[Dict[str, Any]]:
        # Выбираем только активные, где (сейчас - время последнего запуска) >= интервала
        sql = """
        SELECT * FROM kb_watches 
        WHERE is_active = TRUE 
        AND (
            last_run_at IS NULL 
            OR last_run_at <= NOW() - (check_interval || ' seconds')::interval
        )
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql)
            return [dict(row) for row in rows]

    async def update_run_time(self, watch_id: int):
        sql = "UPDATE kb_watches SET last_run_at = NOW() WHERE id = $1"
        async with self.pool.acquire() as conn:
            await conn.execute(sql, watch_id)

    async def get_active_bots(self) -> List[str]:
        sql = "SELECT token FROM bots_managed WHERE is_active = TRUE"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql)
            return [row['token'] for row in rows]
            
    async def get_watch_channel_id(self, watch_id: int) -> str:
        """Получает Chat ID канала, привязанного к воркеру"""
        sql = """
            SELECT c.chat_id 
            FROM kb_watches w 
            JOIN channels c ON w.channel_id = c.id 
            WHERE w.id = $1
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, watch_id)