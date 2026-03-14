from aiohttp import web
import orjson
from aiogram import Bot
from aiogram_bot_template.utils.auth import verify_password, hash_password, create_token # Убедитесь, что функции импортированы
from aiogram_bot_template.utils.kb_api import convert_list_to_api, fetch_cars
from aiogram_bot_template.db.db_api.watches import WatchesRepo
from datetime import datetime
import structlog

logger = structlog.get_logger()

routes = web.RouteTableDef()


async def _resolve_channel_fk(conn, channel_id: object) -> int | None:
    if channel_id is None:
        return None

    channel_id_str = str(channel_id).strip()
    if not channel_id_str:
        return None

    try:
        channel_id_int = int(channel_id_str)
    except Exception:
        channel_id_int = None

    if channel_id_int is not None:
        exists = await conn.fetchval("SELECT 1 FROM channels WHERE id = $1", channel_id_int)
        if exists:
            return channel_id_int

    resolved_id = await conn.fetchval("SELECT id FROM channels WHERE chat_id = $1", channel_id_str)
    if resolved_id:
        return int(resolved_id)

    raise web.HTTPBadRequest(
        text=orjson.dumps(
            {
                "error": "Канал не найден. Передайте channelId как ID из /channels (поле id) или как Telegram chat_id (поле chatId).",
            }
        ).decode(),
        content_type="application/json",
    )

# Хелпер для сериализации данных (обработка datetime и прочего)
def json_response(data, status=200):
    return web.json_response(
        data, 
        status=status, 
        dumps=lambda obj: orjson.dumps(
            obj, 
            option=orjson.OPT_PASSTHROUGH_DATETIME,
            default=lambda x: x.isoformat() if isinstance(x, datetime) else str(x)
        ).decode()
    )

# --- AUTH ---
@routes.post("/login")
async def login_handler(request: web.Request):
    try:
        data = await request.json()
        username = data.get("username")
        password = data.get("password")

        if not username or not password:
            return json_response({"success": False, "error": "Введите логин и пароль"}, status=400)

        db_pool = request.app["db_pool"]
        
        async with db_pool.acquire() as conn:
            # Ищем пользователя в БД
            user = await conn.fetchrow(
                "SELECT username, password_hash FROM admin_users WHERE username = $1", 
                username
            )

            # Если пользователь найден и пароль совпадает
            if user and verify_password(password, user['password_hash']):
                # Создаем реальный JWT токен
                token = create_token(username)
                
                logger.info(f"Успешный вход: {username}")
                return json_response({
                    "success": True, 
                    "token": token
                })

        # Если неверный логин или пароль
        logger.warning(f"Неудачная попытка входа: {username}")
        return json_response({
            "success": False, 
            "error": "Неверный логин или пароль"
        }, status=401)

    except Exception as e:
        logger.error(f"Ошибка при авторизации: {e}")
        return json_response({"success": False, "error": "Ошибка сервера"}, status=500)


@routes.post("/change-password")
async def change_password_handler(request: web.Request):
    try:
        data = await request.json()
        current_password = data.get("currentPassword")
        new_password = data.get("newPassword")
        
        # Получаем текущего пользователя (обычно из JWT, но для простоты возьмем admin)
        db_pool = request.app["db_pool"]
        async with db_pool.acquire() as conn:
            user = await conn.fetchrow("SELECT * FROM admin_users WHERE username = 'admin'")
            
            if not user or not verify_password(current_password, user['password_hash']):
                return json_response({"success": False, "error": "Текущий пароль неверен"}, status=400)
            
            # Хешируем новый пароль и сохраняем
            new_hash = hash_password(new_password)
            await conn.execute("UPDATE admin_users SET password_hash = $1 WHERE username = 'admin'", new_hash)
            
        return json_response({"success": True})
    except Exception as e:
        return json_response({"success": False, "error": str(e)}, status=500)

# --- CHANNELS ---
@routes.get("/channels")
async def get_channels_handler(request: web.Request):
    db_pool = request.app["db_pool"]
    async with db_pool.acquire() as conn:
        # Важно: в Postgres boolean возвращается как True/False, 
        # aiohttp автоматически сконвертирует это в JSON true/false
        rows = await conn.fetch("""
            SELECT id, title, username, chat_id as "chatId", is_default as "isDefault" 
            FROM channels 
            ORDER BY is_default DESC, id ASC
        """)
        return json_response([dict(r) for r in rows])


@routes.post("/channels")
async def create_channel_handler(request: web.Request):
    data = await request.json()
    identifier = data.get("chat_id") # Сюда фронт может прислать @username или ID
    bot: Bot = request.app["bot"]
    db_pool = request.app["db_pool"]

    try:
        # Резолвим данные через Telegram
        chat = await bot.get_chat(identifier)
        
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO channels (title, username, chat_id) 
                VALUES ($1, $2, $3) 
                ON CONFLICT (chat_id) DO UPDATE SET title = EXCLUDED.title, username = EXCLUDED.username
                RETURNING id, title, username, chat_id as "chatId", is_default as "isDefault"
            """, chat.title, f"@{chat.username}" if chat.username else "private", str(chat.id))
            
            return json_response(dict(row))
    except Exception as e:
        return json_response({"error": f"ID/Username не найден: {str(e)}"}, status=400)

# --- BOTS ---
@routes.get("/bots")
async def get_bots_handler(request: web.Request):
    db_pool = request.app["db_pool"]
    async with db_pool.acquire() as conn:
        try:
            # Выбираем все необходимые поля
            rows = await conn.fetch("""
                SELECT 
                    id, 
                    name, 
                    token, 
                    username, 
                    channel_id as "channelId",
                    is_active as "isActive"
                FROM bots_managed 
                ORDER BY id ASC
            """)
            
            # Превращаем в список словарей
            bots = [dict(r) for r in rows]
            
            # Для фронтенда добавим статус доступа (verified), если бот активен
            for b in bots:
                b["accessStatus"] = "verified" if b.get("isActive") else "no_access"
                
            return json_response(bots)
        except Exception as e:
            logger.error(f"Error getting bots from DB: {e}")
            return json_response([])


@routes.post("/bots")
async def add_bot_handler(request: web.Request):
    data = await request.json()
    token = data.get("token")
    name = data.get("name", "New Bot")
    channel_id = data.get("channelId")

    try:
        # The backend fetches the TRUTH from Telegram API
        async with Bot(token=token) as temp_bot:
            me = await temp_bot.get_me()
            official_username = f"@{me.username}"

        db_pool = request.app["db_pool"]
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO bots_managed (name, token, username, channel_id)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (token) DO UPDATE 
                SET name = EXCLUDED.name, 
                    username = EXCLUDED.username, 
                    channel_id = EXCLUDED.channel_id
                RETURNING id, name, token, username, channel_id as "channelId"
            """, name, token, official_username, int(channel_id) if channel_id else None)
            
            return json_response(dict(row))
    except Exception as e:
        return json_response({"error": f"Invalid Bot Token: {str(e)}"}, status=400)

@routes.post("/bots/check_access")
async def check_bot_access_handler(request: web.Request):
    data = await request.json()
    token = data.get("token")
    chat_id = data.get("chat_id")
    
    try:
        async with Bot(token=token) as temp_bot:
            me = await temp_bot.get_me()
            member = await temp_bot.get_chat_member(chat_id=chat_id, user_id=me.id)
            has_access = member.status in ["administrator", "creator"]
            return json_response({"hasAccess": has_access})
    except Exception as e:
        return json_response({"hasAccess": False, "error": str(e)})

@routes.delete("/bots/{id}")
async def delete_bot(request: web.Request):
    bot_id = int(request.match_info['id'])
    db_pool = request.app["db_pool"]
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM bots_managed WHERE id = $1", bot_id)
    return json_response({"status": "deleted"})

# --- WATCHERS ---

@routes.get("/watchers")
async def get_watchers_handler(request: web.Request):
    db_pool = request.app["db_pool"]
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT 
                id, name, original_url as "sourceUrl", 
                api_url as "generatedUrl", channel_id as "channelId", 
                check_interval as "interval", is_active, 
                created_at as "createdAt", last_run_at as "lastCheck"
            FROM kb_watches ORDER BY id DESC
        """)
        
        watchers = []
        for r in rows:
            d = dict(r)
            d["status"] = "running" if d.pop("is_active") else "stopped"
            d["verificationStatus"] = "verified"
            # КРИТИЧНО: Превращаем в строку для фронтенда
            d["channelId"] = str(d["channelId"]) if d["channelId"] else ""
            watchers.append(d)
        return json_response(watchers)

@routes.post("/watchers")
async def add_watch_handler(request: web.Request):
    data = await request.json()
    list_url = data.get("url")
    name = data.get("name")
    channel_id = data.get("channelId")
    interval = int(data.get("interval", 60))

    api_url = convert_list_to_api(list_url)
    db_pool = request.app["db_pool"]
    parsing_session = request.app["parsing_session"]
    
    # 1. Проверка первой страницы (быстрый ответ)
    api_data = await fetch_cars(parsing_session, api_url, max_pages=1)
    if not api_data or not api_data.get("list"):
        return json_response({"error": "На первой странице нет авто или URL неверный"}, status=400)

    last_seq = str(api_data["list"][0]["carSeq"])
 
    async with db_pool.acquire() as conn:
        resolved_channel_id = await _resolve_channel_fk(conn, channel_id)
        watch_id = await conn.fetchval("""
            INSERT INTO kb_watches (name, original_url, api_url, last_car_seq, check_interval, channel_id, is_active) 
            VALUES ($1, $2, $3, $4, $5, $6, TRUE) RETURNING id
        """, name, list_url, api_url, last_seq, interval, resolved_channel_id)
        
        # Сидим только первую страницу, чтобы не спамить
        await conn.executemany("""
            INSERT INTO kb_seen_cars (watch_id, car_history_seq, car_seq)
            VALUES ($1, $2, $3) ON CONFLICT DO NOTHING
        """, [(watch_id, str(c.get("carHistorySeq") or "0"), str(c["carSeq"])) for c in api_data["list"]])

    return json_response({
        "id": watch_id,
        "name": name,
        "sourceUrl": list_url,
        "generatedUrl": api_url,
        "channelId": str(resolved_channel_id) if resolved_channel_id else "",
        "status": "running",
        "verificationStatus": "verified"
    })  

@routes.patch("/watchers/{id}/status")
async def toggle_watcher_status_handler(request: web.Request):
    try:
        watcher_id = int(request.match_info['id'])
        data = await request.json()
        # Фронтенд присылает статус "running" или "stopped"
        new_status = data.get("status")
        is_active = (new_status == "running")

        db_pool = request.app["db_pool"]
        async with db_pool.acquire() as conn:
            # Обновляем поле is_active в базе данных
            result = await conn.execute(
                "UPDATE kb_watches SET is_active = $1 WHERE id = $2",
                is_active, watcher_id
            )
            
            if "UPDATE 0" in result:
                return json_response({"error": "Watcher not found"}, status=404)

        return json_response({"status": "ok", "id": watcher_id, "is_active": is_active})
    except Exception as e:
        return json_response({"error": str(e)}, status=500)

# --- УДАЛЕНИЕ ---
@routes.delete("/watchers/{id}")
async def delete_watcher_handler(request: web.Request):
    try:
        watcher_id = int(request.match_info['id'])
        db_pool = request.app["db_pool"]
        
        async with db_pool.acquire() as conn:
            # Благодаря ON DELETE CASCADE в структуре таблицы, 
            # история увиденных машин (kb_seen_cars) удалится автоматически
            result = await conn.execute("DELETE FROM kb_watches WHERE id = $1", watcher_id)
            
            if "DELETE 0" in result:
                return json_response({"error": "Watcher not found"}, status=404)
                
        return json_response({"status": "deleted", "id": watcher_id})
    except Exception as e:
        return json_response({"error": str(e)}, status=500)

# --- ОБНОВЛЕНИЕ (РЕДАКТИРОВАНИЕ) ---
@routes.put("/watchers/{id}")
async def update_watcher_handler(request: web.Request):
    try:
        watcher_id = int(request.match_info['id'])
        data = await request.json()
        
        db_pool = request.app["db_pool"]
        parsing_session = request.app["parsing_session"]

        # Получаем данные из запроса
        name = data.get("name")
        list_url = data.get("url")
        channel_id = data.get("channelId")
        interval = data.get("interval")

        async with db_pool.acquire() as conn:
            resolved_channel_id = await _resolve_channel_fk(conn, channel_id)
            # 1. Если URL изменился, нужно перегенерировать API URL и сбросить историю
            if list_url:
                api_url = convert_list_to_api(list_url)
                # Получаем новый стартовый seq
                api_data = await fetch_cars(parsing_session, api_url, max_pages=1)
                last_seq = str(api_data["list"][0]["carSeq"]) if api_data and api_data.get("list") else None
                
                await conn.execute("""
                    UPDATE kb_watches SET 
                        name = $1, original_url = $2, api_url = $3, 
                        channel_id = $4, check_interval = $5, last_car_seq = $6
                    WHERE id = $7
                """, name, list_url, api_url, resolved_channel_id, 
                int(interval) if interval else 60, last_seq, watcher_id)
            else:
                # 2. Если URL не менялся, обновляем только метаданные
                await conn.execute("""
                    UPDATE kb_watches SET 
                        name = $1, channel_id = $2, check_interval = $3
                    WHERE id = $4
                """, name, resolved_channel_id, 
                int(interval) if interval else 60, watcher_id)

        return json_response({"status": "updated"})
    except Exception as e:
        return json_response({"error": str(e)}, status=500)


@routes.get("/notification-users")
async def get_notification_users_handler(request: web.Request):
    db_pool = request.app["db_pool"]
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, username as \"telegramUsername\", created_at as \"createdAt\" FROM notification_users")
        return json_response([dict(r) for r in rows])


@routes.post("/notification-users")
async def add_notification_user_handler(request: web.Request):
    data = await request.json()
    identifier = str(data.get("telegramUsername")).strip() # Может быть "123456" или "@username"
    name = data.get("name")
    bot: Bot = request.app["bot"]
    db_pool = request.app["db_pool"]

    try:
        # 1. Пытаемся получить данные чата/пользователя
        # Telegram API понимает и числовые ID, и юзернеймы (если бот их "видел")
        chat = await bot.get_chat(identifier)
        chat_id = str(chat.id)
        username = f"@{chat.username}" if chat.username else f"ID: {chat_id}"

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO notification_users (name, username, chat_id) 
                VALUES ($1, $2, $3) 
                ON CONFLICT (chat_id) DO UPDATE SET name = EXCLUDED.name, username = EXCLUDED.username
                RETURNING id, name, username as "telegramUsername", created_at as "createdAt"
            """, name, username, chat_id)
            return json_response(dict(row))
            
    except Exception as e:
        # Если get_chat упал, значит пользователь не начинал диалог с ботом
        logger.error(f"Error adding notification user: {e}")
        return json_response({
            "error": "Пользователь не найден. Инструкция: 1. Напишите боту /start. 2. Введите полученный ID в это поле."
        }, status=400)


@routes.delete("/notification-users/{id}")
async def delete_notification_user_handler(request: web.Request):
    user_id = int(request.match_info['id'])
    db_pool = request.app["db_pool"]
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM notification_users WHERE id = $1", user_id)
    return json_response({"status": "deleted"})

