import jwt
import datetime
import bcrypt

SECRET_KEY = "YOUR_SUPER_SECRET_KEY" # Вынести в config.py


def hash_password(password: str) -> str:
    """Хеширует пароль с солью."""
    # bcrypt принимает байты, поэтому кодируем строку
    pw_bytes = password.encode('utf-8')
    # Генерируем соль и хешируем
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(pw_bytes, salt)
    # Возвращаем строку для хранения в БД
    return hashed.decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    """Проверяет соответствие пароля хешу."""
    try:
        if not hashed:
            return False
        # Проверяем пароль (оба аргумента должны быть байтами)
        return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
    except Exception as e:
        print(f"Bcrypt verification error: {e}")
        return False

def create_token(username: str):
    payload = {
        "sub": username,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=24)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")