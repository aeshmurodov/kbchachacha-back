# aiogram_bot_template/handlers/user/start.py
from aiogram import html, types
from aiogram.fsm.context import FSMContext
from aiogram_bot_template import states

async def start(msg: types.Message, state: FSMContext) -> None:
    if msg.from_user is None:
        return
        
    user_id = msg.from_user.id
    full_name = html.quote(msg.from_user.full_name)
    
    m = [
        f"👋 Привет, {full_name}!",
        f"Ваш Telegram ID: <code>{user_id}</code>",
        "",
        "Отправьте этот ID администратору или вставьте в панель управления, чтобы получать уведомления об ошибках."
    ]
    
    await msg.answer("\n".join(m))
    await state.set_state(states.user.UserMainMenu.menu)