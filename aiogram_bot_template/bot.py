import asyncio
import sys
from typing import TYPE_CHECKING

import aiojobs
import aiohttp  # <--- ДОБАВИТЬ ЭТУ СТРОКУ 
import aiohttp_cors # Импортируйте
import orjson
import tenacity
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.telegram import TelegramAPIServer
from aiogram.fsm.storage.redis import DefaultKeyBuilder, RedisStorage
from aiohttp import web
from redis.asyncio import Redis

from aiogram_bot_template import handlers, utils, web_handlers
from aiogram_bot_template.data import config
from aiogram_bot_template.middlewares import StructLoggingMiddleware

from apscheduler.schedulers.asyncio import AsyncIOScheduler # Добавить импорт
from aiogram_bot_template.web_handlers import external_api # Добавить импорт
from aiogram_bot_template.services.kb_poller import check_updates # Добавить импорт
from aiogram_bot_template.utils.auth import hash_password # Убедитесь, что путь верный

if TYPE_CHECKING:
    import asyncpg
    import structlog
    from aiogram.client.session.aiohttp import AiohttpSession


async def create_db_connections(dp: Dispatcher) -> None:
    logger: structlog.typing.FilteringBoundLogger = dp["business_logger"]

    logger.debug("Connecting to PostgreSQL", db="main")
    try:
        db_pool = await utils.connect_to_services.wait_postgres(
            logger=dp["db_logger"],
            dsn=config.PG_LINK,
        )
    except tenacity.RetryError:
        logger.exception("Failed to connect to PostgreSQL", db="main")
        sys.exit(1)
    else:
        logger.debug("Succesfully connected to PostgreSQL", db="main")
    dp["db_pool"] = db_pool

    async with db_pool.acquire() as conn:

                # 1. СИСТЕМНЫЕ ТАБЛИЦЫ (Админка и Каналы)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS admin_users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL
            );
        """)

        # 1. Создаем таблицу каналов (нужна для связи)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id SERIAL PRIMARY KEY,
                title TEXT,
                username TEXT,
                chat_id TEXT UNIQUE NOT NULL,
                is_default BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # 2. Создаем таблицу ботов с полем username
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bots_managed (
                id SERIAL PRIMARY KEY,
                name TEXT,
                token TEXT UNIQUE NOT NULL,
                username TEXT, -- Новое поле
                is_active BOOLEAN DEFAULT TRUE,
                channel_id INTEGER REFERENCES channels(id) ON DELETE SET NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # 3. АВТО-РЕГИСТРАЦИЯ встроенного бота
        # Чтобы получить username, создаем временный объект Bot
        async with Bot(token=config.BOT_TOKEN) as temp_bot:
            try:
                me = await temp_bot.get_me()
                bot_username = f"@{me.username}"
            except Exception:
                bot_username = "@unknown_bot"

        # 3. ВОРКЕРЫ (Наблюдатели)
        # Сначала создаем саму таблицу воркеров
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS kb_watches (
                id SERIAL PRIMARY KEY,
                user_id BIGINT DEFAULT 0,
                name TEXT,
                original_url TEXT,
                api_url TEXT,
                last_car_seq TEXT,
                check_interval INTEGER DEFAULT 60,
                is_active BOOLEAN DEFAULT TRUE,
                last_run_at TIMESTAMPTZ,
                channel_id INTEGER REFERENCES channels(id) ON DELETE SET NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # 4. ИСТОРИЯ ПРОСМОТРОВ (Чтобы не дублировать авто)
        # Оптимизированный индекс по (watch_id, car_history_seq)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS kb_seen_cars (
                watch_id INTEGER NOT NULL REFERENCES kb_watches(id) ON DELETE CASCADE,
                car_history_seq TEXT NOT NULL,
                car_seq TEXT NOT NULL,
                seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (watch_id, car_history_seq)
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS notification_users (
                id SERIAL PRIMARY KEY,
                name TEXT,
                username TEXT,
                chat_id TEXT UNIQUE NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)


        # Вставляем или обновляем данные основного бота
        await conn.execute("""
            INSERT INTO bots_managed (name, token, username, is_active)
            VALUES ($1, $2, $3, TRUE)
            ON CONFLICT (token) DO UPDATE 
            SET username = EXCLUDED.username, is_active = TRUE;
        """, "Main Bot (Env)", config.BOT_TOKEN, bot_username)


                # 2. РЕЗОЛВИНГ ДАННЫХ ИЗ ENV (Бот и Канал)
        main_channel_db_id = None
        async with Bot(token=config.BOT_TOKEN) as temp_bot:
            # Получаем данные основного бота
            me = await temp_bot.get_me()
            bot_username = f"@{me.username}"

            # Получаем данные канала из ENV, если он указан
            if config.MAIN_CHANNEL:
                try:
                    chat = await temp_bot.get_chat(config.MAIN_CHANNEL)
                    # Сохраняем/обновляем канал в БД
                    channel_row = await conn.fetchrow("""
                        INSERT INTO channels (title, username, chat_id, is_default)
                        VALUES ($1, $2, $3, TRUE)
                        ON CONFLICT (chat_id) DO UPDATE 
                        SET title = EXCLUDED.title, username = EXCLUDED.username
                        RETURNING id
                    """, chat.title, f"@{chat.username}" if chat.username else "private", str(chat.id))
                    main_channel_db_id = channel_row['id']
                except Exception as e:
                    dp["aiogram_logger"].error(f"Could not resolve MAIN_CHANNEL: {e}")

        # 3. РЕГИСТРАЦИЯ/ОБНОВЛЕНИЕ основного бота
        await conn.execute("""
            INSERT INTO bots_managed (name, token, username, is_active, channel_id)
            VALUES ($1, $2, $3, TRUE, $4)
            ON CONFLICT (token) DO UPDATE 
            SET username = EXCLUDED.username, 
                channel_id = COALESCE(EXCLUDED.channel_id, bots_managed.channel_id),
                is_active = TRUE;
        """, "Main Bot (Env)", config.BOT_TOKEN, bot_username, main_channel_db_id)


        # --- АВТОМАТИЧЕСКОЕ СОЗДАНИЕ АДМИНА ---
        # Проверяем, есть ли вообще пользователи в таблице
        admin_count = await conn.fetchval("SELECT COUNT(*) FROM admin_users")
        
        if admin_count == 0:
            default_username = "admin"
            default_password = "admin"
            # Хешируем пароль через нашу функцию (использует bcrypt)
            hashed_pw = hash_password(default_password)
            
            await conn.execute(
                "INSERT INTO admin_users (username, password_hash) VALUES ($1, $2)",
                default_username, hashed_pw
            )
            dp["aiogram_logger"].info("Default admin user created (admin/admin)")
        # ---------------------------------------
            
    # --- ОСТАЛЬНАЯ ЧАСТЬ (REDIS И СЕССИИ) ---
    if config.USE_CACHE:
        logger.debug("Connecting to Redis")
        try:
            redis_pool = await utils.connect_to_services.wait_redis_pool(
                logger=dp["cache_logger"],
                host=config.CACHE_HOST,
                username=config.CACHE_USERNAME,
                password=config.CACHE_PASSWORD,
                port=config.CACHE_PORT,
                database=0,
            )
        except tenacity.RetryError:
            logger.exception("Failed to connect to Redis")
            sys.exit(1)
        else:
            logger.debug("Succesfully connected to Redis")
        dp["cache_pool"] = redis_pool

    # Сессии для aiogram
    dp["temp_bot_cloud_session"] = utils.smart_session.SmartAiogramAiohttpSession(
        json_loads=orjson.loads,
        logger=dp["aiogram_session_logger"],
    )
    if config.USE_CUSTOM_API_SERVER:
        dp["temp_bot_local_session"] = utils.smart_session.SmartAiogramAiohttpSession(
            api=TelegramAPIServer(
                base=config.CUSTOM_API_SERVER_BASE,
                file=config.CUSTOM_API_SERVER_FILE,
                is_local=config.CUSTOM_API_SERVER_IS_LOCAL,
            ),
            json_loads=orjson.loads,
            logger=dp["aiogram_session_logger"],
        )


async def close_db_connections(dp: Dispatcher) -> None:
    if "temp_bot_cloud_session" in dp.workflow_data:
        temp_bot_cloud_session: AiohttpSession = dp["temp_bot_cloud_session"]
        await temp_bot_cloud_session.close()
    if "temp_bot_local_session" in dp.workflow_data:
        temp_bot_local_session: AiohttpSession = dp["temp_bot_local_session"]
        await temp_bot_local_session.close()
    if "db_pool" in dp.workflow_data:
        db_pool: asyncpg.Pool = dp["db_pool"]
        await db_pool.close()
    if "cache_pool" in dp.workflow_data:
        cache_pool: Redis = dp["cache_pool"]
        await cache_pool.close()


def setup_handlers(dp: Dispatcher) -> None:
    dp.include_router(handlers.user.prepare_router())


def setup_middlewares(dp: Dispatcher) -> None:
    dp.update.outer_middleware(StructLoggingMiddleware(logger=dp["aiogram_logger"]))


def setup_logging(dp: Dispatcher) -> None:
    dp["aiogram_logger"] = utils.logging.setup_logger().bind(type="aiogram")
    dp["db_logger"] = utils.logging.setup_logger().bind(type="db")
    dp["cache_logger"] = utils.logging.setup_logger().bind(type="cache")
    dp["business_logger"] = utils.logging.setup_logger().bind(type="business")


async def setup_aiogram(dp: Dispatcher) -> None:
    setup_logging(dp)
    logger = dp["aiogram_logger"]
    logger.debug("Configuring aiogram")
    await create_db_connections(dp)
    setup_handlers(dp)
    setup_middlewares(dp)
    logger.info("Configured aiogram")


async def aiohttp_on_startup(app: web.Application) -> None:
    dp: Dispatcher = app["dp"]
    
    # 1. Получаем пул БД из диспетчера
    # Важно: убедитесь, что create_db_connections уже выполнился к этому моменту.
    # В текущей архитектуре aiogram_on_startup_webhook вызывается ПЕРЕД этим, так что пул уже должен быть в dp["db_pool"]
    # Но надежнее взять его из workflow_data, если он там есть, или из dp
    db_pool = dp.get("db_pool")
    
    # 2. Создаем нормальную aiohttp сессию для парсинга
    parsing_session = aiohttp.ClientSession()
    
    # Прокидываем во все приложения
    apps_to_configure = [app, *app._subapps]
    for _app in apps_to_configure:
        _app["db_pool"] = db_pool
        _app["parsing_session"] = parsing_session  # Сохраняем под новым именем

    # Регистрируем закрытие сессии при выключении
    async def close_session(app):
        await parsing_session.close()
    app.on_cleanup.append(close_session)

    workflow_data = {"app": app, "dispatcher": dp}
    if "bot" in app:
        workflow_data["bot"] = app["bot"]
    await dp.emit_startup(**workflow_data)


async def aiohttp_on_shutdown(app: web.Application) -> None:
    dp: Dispatcher = app["dp"]
    for i in [app, *app._subapps]:  # noqa: SLF001 # dirty
        if "scheduler" in i:
            scheduler: aiojobs.Scheduler = i["scheduler"]
            scheduler._closed = True  # noqa: SLF001
            while scheduler.pending_count != 0:
                dp["aiogram_logger"].info(
                    f"Waiting for {scheduler.pending_count} tasks to complete",
                )
                await asyncio.sleep(1)
    workflow_data = {"app": app, "dispatcher": dp}
    if "bot" in app:
        workflow_data["bot"] = app["bot"]
    await dp.emit_shutdown(**workflow_data)


async def aiogram_on_startup_webhook(dispatcher: Dispatcher, bot: Bot) -> None:
    await setup_aiogram(dispatcher)
    webhook_logger = dispatcher["aiogram_logger"].bind(
        webhook_url=config.MAIN_WEBHOOK_ADDRESS,
    )
    webhook_logger.debug("Configuring webhook")
    await bot.set_webhook(
        url=config.MAIN_WEBHOOK_ADDRESS.format(
            token=config.BOT_TOKEN,
            bot_id=config.BOT_TOKEN.split(":")[0],
        ),
        allowed_updates=dispatcher.resolve_used_update_types(),
        secret_token=config.MAIN_WEBHOOK_SECRET_TOKEN,
    )
    webhook_logger.info("Configured webhook")


async def aiogram_on_shutdown_webhook(dispatcher: Dispatcher, bot: Bot) -> None:
    dispatcher["aiogram_logger"].debug("Stopping webhook")
    await close_db_connections(dispatcher)
    await bot.session.close()
    await dispatcher.storage.close()
    dispatcher["aiogram_logger"].info("Stopped webhook")


async def aiohttp_on_startup(app: web.Application) -> None:
    dp: Dispatcher = app["dp"]
    bot: Bot = app["bot"]
    
    # 1. СНАЧАЛА запускаем стартап диспетчера. 
    # Это вызовет aiogram_on_startup_webhook -> setup_aiogram -> create_db_connections
    workflow_data = {"app": app, "dispatcher": dp, "bot": bot}
    await dp.emit_startup(**workflow_data)
    
    # 2. ТЕПЕРЬ пул точно создан и лежит в dp["db_pool"]
    db_pool = dp.get("db_pool")
    
    # 3. Создаем сессию для парсинга и кладем в app
    parsing_session = aiohttp.ClientSession()
    
    # Раздаем ссылки всем под-приложениям
    for _app in [app, *app._subapps]:
        _app["db_pool"] = db_pool
        _app["parsing_session"] = parsing_session

    # Не забываем закрыть сессию при выходе
    async def close_session(app):
        await parsing_session.close()
    app.on_cleanup.append(close_session)

async def aiogram_on_shutdown_polling(dispatcher: Dispatcher, bot: Bot) -> None:
    dispatcher["aiogram_logger"].debug("Stopping polling")
    await close_db_connections(dispatcher)
    await bot.session.close()
    await dispatcher.storage.close()
    dispatcher["aiogram_logger"].info("Stopped polling")


async def setup_aiohttp_app(bot: Bot, dp: Dispatcher) -> web.Application:
    scheduler = aiojobs.Scheduler()
    app = web.Application()
    
    # --- НОВЫЙ КОД: Саб-приложение для внешнего API ---
    api_app = web.Application()
    api_app.add_routes(external_api.routes)
    # Передаем зависимости в api_app
    api_app["bot"] = bot
    api_app["dp"] = dp
    # Пул БД будет доступен, так как мы его прокинем ниже

    # Настройка CORS
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
            allow_methods="*",
        )
    })
    
    subapps: list[tuple[str, web.Application]] = [
        ("/tg/webhooks/", web_handlers.tg_updates_app),
        ("/api/", api_app), # Подключаем наш API по пути /api/
    ]
    # --------------------------------------------------

    for prefix, subapp in subapps:
        subapp["bot"] = bot
        subapp["dp"] = dp
        subapp["scheduler"] = scheduler
        app.add_subapp(prefix, subapp)

    # Применяем CORS ко всем роутам API
    for route in list(api_app.router.routes()):
        cors.add(route)
        
    app["bot"] = bot
    app["dp"] = dp
    app["scheduler"] = scheduler
    app.on_startup.append(aiohttp_on_startup)
    app.on_shutdown.append(aiohttp_on_shutdown)
    return app


def main() -> None:
    aiogram_session_logger = utils.logging.setup_logger().bind(type="aiogram_session")

    if config.USE_CUSTOM_API_SERVER:
        session = utils.smart_session.SmartAiogramAiohttpSession(
            api=TelegramAPIServer(
                base=config.CUSTOM_API_SERVER_BASE,
                file=config.CUSTOM_API_SERVER_FILE,
                is_local=config.CUSTOM_API_SERVER_IS_LOCAL,
            ),
            json_loads=orjson.loads,
            logger=aiogram_session_logger,
        )
    else:
        session = utils.smart_session.SmartAiogramAiohttpSession(
            json_loads=orjson.loads,
            logger=aiogram_session_logger,
        )
    bot = Bot(
        config.BOT_TOKEN,
        session=session,
        default=DefaultBotProperties(parse_mode="HTML"),
    )

    dp = Dispatcher(
        storage=RedisStorage(
            redis=Redis(
                host=config.FSM_HOST,
                password=config.FSM_PASSWORD,
                port=config.FSM_PORT,
                db=0,
            ),
            key_builder=DefaultKeyBuilder(with_bot_id=True),
        ),
    )
    dp["aiogram_session_logger"] = aiogram_session_logger
    
    scheduler = AsyncIOScheduler()

    async def run_check():
        # Проверяем, подключена ли БД. В режиме Webhook данные могут быть в app, а не в workflow_data напрямую при старте,
        # но dp["db_pool"] мы прокинули в aiohttp_on_startup.
        # Безопаснее брать из workflow_data, если доступно, или проверять наличие.
        pool = dp.get("db_pool")
        if pool:
            # Создаем сессию с таймаутом, чтобы она не висела вечно
            timeout = aiohttp.ClientTimeout(total=240) 
            async with aiohttp.ClientSession(timeout=timeout) as session:
                try:
                    await check_updates(bot, pool, session)
                except Exception as e:
                    logger = dp.get("business_logger")
                    if logger:
                        logger.exception("Scheduled KB check failed", error=str(e))
                # Даем немного времени на закрытие соединений внутри контекста
                await asyncio.sleep(1)

    # Добавляем задачу
    scheduler.add_job(
        run_check,
        "interval",
        seconds=10,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )

    # === ВАЖНОЕ ИСПРАВЛЕНИЕ ===
    # Создаем функцию запуска планировщика
    async def on_startup_scheduler():
        scheduler.start()
    
    # Регистрируем её, чтобы она запустилась КОГДА бот уже начнет работу
    dp.startup.register(on_startup_scheduler)
    # ==========================

    if config.USE_WEBHOOK:
        dp.startup.register(aiogram_on_startup_webhook)
        dp.shutdown.register(aiogram_on_shutdown_webhook)
        
        # Мы создаем цикл событий вручную, чтобы контролировать его
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Создаем приложение
        app = loop.run_until_complete(setup_aiohttp_app(bot, dp))
        
        # Запускаем сервер
        web.run_app(
            app,
            handle_signals=True,
            host=config.MAIN_WEBHOOK_LISTENING_HOST,
            port=config.MAIN_WEBHOOK_LISTENING_PORT,
        )
    else:
        dp.startup.register(aiogram_on_startup_polling)
        dp.shutdown.register(aiogram_on_shutdown_polling)
        asyncio.run(dp.start_polling(bot))


if __name__ == "__main__":
    main()
