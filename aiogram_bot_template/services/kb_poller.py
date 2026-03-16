import asyncio
from datetime import datetime, timedelta, timezone
from aiogram import Bot
from aiogram.types import InputMediaPhoto
import aiohttp
import structlog

from aiogram_bot_template.db.db_api.watches import WatchesRepo
from aiogram_bot_template.utils.kb_api import fetch_cars
from aiogram.client.default import DefaultBotProperties


logger = structlog.get_logger()

_CHECK_LOCK = asyncio.Lock()
# Храним экземпляры ботов, чтобы не создавать их каждую секунду
_BOT_INSTANCES = {}

KST = timezone(timedelta(hours=9))


def _kb_image_path(filename: str) -> str | None:
    if not filename:
        return None

    car_seq = filename.split("_", maxsplit=1)[0]
    if len(car_seq) < 4:
        return None

    mid = car_seq[3]
    sub = car_seq[:4]
    mid_folder = "img10" if mid == "0" else f"img0{mid}"
    return f"/IMG/carimg/l/{mid_folder}/img{sub}/{filename}"


def _kb_image_url(filename: str, width: int = 720) -> str | None:
    path = _kb_image_path(filename)
    if not path:
        return None
    return f"https://img.kbchachacha.com{path}?width={width}"


def _get_car_image_urls(car: dict, limit: int = 10) -> list[str]:
    filenames = car.get("fileNameArray")
    if not isinstance(filenames, list) or not filenames:
        return []

    urls: list[str] = []
    for filename in filenames:
        if len(urls) >= limit:
            break
        if not isinstance(filename, str):
            continue
        url = _kb_image_url(filename)
        if url:
            urls.append(url)
    return urls


def _parse_order_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        # 1. fromisoformat поймет 'Z' в конце и сделает его UTC автоматически
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        
        # 2. Если зона не указана (как в "2026-02-20 21:49:33"), 
        # считаем, что это корейское время (KST)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
            
        # 3. Возвращаем всё в UTC для сравнения в коде
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _build_car_message(title: str, car: dict) -> str:
    price = car.get("sellAmt", "N/A")
    name = f"{car.get('makerName', '')} {car.get('modelName', '')} {car.get('carName', '')}".strip()
    grade = car.get("gradeName")
    yymm = car.get("yymm", "N/A")
    km = car.get("km", "N/A")
    city = car.get("cityCodeName2")
    gas = car.get("gasName")
    premium_text = car.get("paymentPremiumText")
    seller_name = car.get("tbMemberMemberName")
    seller_tel = car.get("safeTel")
    link = f"https://www.kbchachacha.com/public/car/detail.kbc?carSeq={car['carSeq']}"

    order_dt = _parse_order_date(car.get("orderDate"))
    if order_dt:
        # Отображаем время в корейском формате (KST), так привычнее для этого рынка
        # Или замените на ваш локальный пояс
        order_date_display = order_dt.astimezone(KST).strftime("%d.%m %H:%M") + " (KST)"
    else:
        order_date_display = "Н/Д"

    lines: list[str] = [
        title,
        f"<b>{name}</b>",
    ]
    if grade:
        lines.append(str(grade))
    lines.extend(
        [
            f"💰 Цена: {price} KRW",
            f"📅 Год: {yymm}",
            f"📟 Пробег: {km}",
        ]
    )
    if city:
        lines.append(f"📍 Город: {city}")
    if gas:
        lines.append(f"⛽ Топливо: {gas}")
    if order_date_display:
        lines.append(f"🕒 Дата: {order_date_display}")
    if premium_text:
        lines.append(f"📝 {premium_text}")
    if seller_name or seller_tel:
        seller_line = "👤 Продавец:"
        if seller_name:
            seller_line += f" {seller_name}"
        if seller_tel:
            seller_line += f" ({seller_tel})" if seller_name else f" {seller_tel}"
        lines.append(seller_line)

    lines.append("")
    lines.append(f"<a href='{link}'>Открыть объявление</a>")
    return "\n".join(lines)

async def get_bot_instance(token: str) -> Bot:
    if token not in _BOT_INSTANCES:
        _BOT_INSTANCES[token] = Bot(
            token=token, 
            default=DefaultBotProperties(parse_mode="HTML")
        )
    return _BOT_INSTANCES[token]

async def notify_admins_about_error(bot: Bot, db_pool, error_msg: str):
    """Рассылает уведомление об ошибке всем админам из таблицы notification_users"""
    async with db_pool.acquire() as conn:
        admins = await conn.fetch("SELECT chat_id FROM notification_users")
    
    for admin in admins:
        try:
            await bot.send_message(admin['chat_id'], f"⚠️ <b>ОШИБКА ПАРСЕРА:</b>\n\n<code>{error_msg}</code>")
        except Exception:
            pass

async def check_updates(bot: Bot, db_pool, session: aiohttp.ClientSession):
    async with _CHECK_LOCK:
        try:

            repo = WatchesRepo(db_pool)

            # Получаем только те, которые пора проверять
            watches = await repo.get_watchers_to_check()
            
            if not watches:
                return

            # Загружаем всех активных ботов из БД
            bot_tokens = await repo.get_active_bots()
            if not bot_tokens:
                logger.error("No active bots found in DB!")
                return

            bot_index = 0  # Для Round-Robin внутри одного цикла проверки

            for watch in watches:
                # Сразу отмечаем время запуска, чтобы другие циклы не подхватили
                await repo.update_run_time(watch["id"])
                
                # Получаем реальный Telegram Chat ID из связанной таблицы каналов
                # Важно: watch["channel_id"] — это внутренний ID из нашей БД
                target_chat_id = await repo.get_watch_channel_id(watch["id"])
                
                if not target_chat_id:
                    logger.error(f"Watcher {watch['id']} не привязан к каналу или канал удален")
                    continue

                data = await fetch_cars(session, watch["api_url"], max_pages=3)

                if not data or "list" not in data or not data["list"]:
                    logger.info(
                        "KB fetch returned empty",
                        watch_id=watch["id"],
                        user_id=watch["user_id"],
                        url=watch["api_url"],
                    )
                    continue

                car_list = data["list"]
                now = datetime.now(timezone.utc)
                cutoff = now - timedelta(hours=48)

                car_list = sorted(
                    car_list,
                    key=lambda c: (_parse_order_date(c.get("orderDate")) or datetime.min.replace(tzinfo=timezone.utc)),
                    reverse=True,
                )

                logger.info(
                    "KB fetch ok",
                    watch_id=watch["id"],
                    user_id=watch["user_id"],
                    cars_count=len(car_list),
                )

                watch_id = watch["id"]
                new_cars: list[dict] = []
                is_first_run = False

                try:
                    async with db_pool.acquire(timeout=10) as conn:
                        logger.info("KB check", watch_id=watch_id)

                        is_first_run = await conn.fetchval(
                            "SELECT NOT EXISTS (SELECT 1 FROM kb_seen_cars WHERE watch_id = $1)",
                            watch_id,
                        )

                        logger.info(
                            "KB first run status",
                            watch_id=watch_id,
                            user_id=watch.get("user_id"),
                            is_first_run=bool(is_first_run),
                        )

                        if is_first_run:
                            await conn.executemany(
                                """
                                INSERT INTO kb_seen_cars (watch_id, car_seq, car_history_seq)
                                VALUES ($1, $2, $3)
                                ON CONFLICT DO NOTHING
                                """,
                                [
                                    (
                                        watch_id,
                                        str(car["carSeq"]),
                                        str(car.get("carHistorySeq") or car.get("carSeq") or "0"),
                                    )
                                    for car in car_list
                                ],
                            )
                            logger.info(
                                "KB watch seeded (first run, no notifications)",
                                watch_id=watch_id,
                                user_id=watch["user_id"],
                                cars_seeded=len(car_list),
                            )
                            continue

                        seen_set = {
                            row["car_history_seq"]
                            for row in await conn.fetch(
                                "SELECT car_history_seq FROM kb_seen_cars WHERE watch_id = $1",
                                watch_id,
                            )
                        }
                        new_cars = [
                            car
                            for car in car_list
                            if str(car.get("carHistorySeq") or car.get("carSeq") or "0") not in seen_set
                        ]

                        if new_cars:
                            await conn.executemany(
                                """
                                INSERT INTO kb_seen_cars (watch_id, car_seq, car_history_seq)
                                VALUES ($1, $2, $3)
                                ON CONFLICT DO NOTHING
                                """,
                                [
                                    (
                                        watch_id,
                                        str(car["carSeq"]),
                                        str(car.get("carHistorySeq") or car.get("carSeq") or "0"),
                                    )
                                    for car in new_cars
                                ],
                            )
                except Exception as e:
                    logger.exception(
                        "KB DB step failed",
                        watch_id=watch_id,
                        user_id=watch.get("user_id"),
                        error=str(e),
                    )

                    await notify_admins_about_error(bot, db_pool, f"Воркер '{watch['name']}' упал: {str(e)}")

                    continue

                if is_first_run:
                    continue

                sendable_new_cars = [
                    car
                    for car in new_cars
                    if (_parse_order_date(car.get("orderDate")) is None)
                    or (_parse_order_date(car.get("orderDate")) >= cutoff)
                ]

                if sendable_new_cars:
                    logger.info(
                        "KB new cars found",
                        watch_id=watch_id,
                        user_id=watch["user_id"],
                        new_cars_count=len(sendable_new_cars),
                    )

                    for car in reversed(sendable_new_cars):
                        # ВЫБОР БОТА (Round-Robin)
                        current_token = bot_tokens[bot_index % len(bot_tokens)]
                        current_bot = await get_bot_instance(current_token)
                        
                        try:
                            msg = _build_car_message("🚗 <b>Новое авто!</b>", car)

                            image_urls = _get_car_image_urls(car, limit=10)
                            if image_urls:
                                try:
                                    media = [InputMediaPhoto(media=url) for url in image_urls]
                                    await current_bot.send_media_group(chat_id=target_chat_id, media=media)
                                except Exception as e:
                                    logger.warning(
                                        "Failed to send media group",
                                        watch_id=watch_id,
                                        car_seq=car.get("carSeq"),
                                        error=str(e),
                                    )

                            await current_bot.send_message(chat_id=target_chat_id, text=msg)
                            logger.info(f"Sent car {car['carSeq']} to channel {target_chat_id}")
                                
                            # После успешной отправки ОДНОГО авто этим ботом, переходим к следующему боту
                            bot_index += 1
                            await asyncio.sleep(1) # Защита от спама
                            
                        except Exception as e:

                            logger.error(f"Bot {current_token[:10]}... failed to send: {e}")
                            # Если бот упал, пробуем отправить это же авто следующим ботом
                            bot_index += 1
                    continue
                if new_cars and not sendable_new_cars:
                    logger.info(
                        "KB new cars filtered out by cutoff",
                        watch_id=watch_id,
                        user_id=watch["user_id"],
                        new_cars_count=len(new_cars),
                        cutoff_hours=48,
                    )
        except Exception as e:
            logger.exception("KB check failed", error=str(e))
            await notify_admins_about_error(bot, db_pool, f"Критическая ошибка планировщика: {str(e)}")
