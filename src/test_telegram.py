import asyncio
import logging
import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message

# Настройка логирования с явным указанием кодировки
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("telegram_test.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Получение токена и ID пользователя
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_USER_ID = os.getenv("TELEGRAM_USER_ID")

# Роутер для команд
router = Router()

@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Привет! Тестовый бот запущен успешно.")
    logger.info(f"Команда /start выполнена от пользователя {message.from_user.id}")

@router.message(Command("test"))
async def cmd_test(message: Message):
    await message.answer("Это тестовая команда!")
    logger.info(f"Команда /test выполнена от пользователя {message.from_user.id}")

async def main():
    # Проверка наличия токена
    if not TELEGRAM_BOT_TOKEN:
        logger.error("Не указан токен Telegram бота в файле .env")
        return
    
    # Вывод информации о токене и ID пользователя
    logger.info(f"Токен: {TELEGRAM_BOT_TOKEN[:5]}... (скрыт)")
    logger.info(f"ID пользователя: {TELEGRAM_USER_ID}")
    
    # Инициализация бота и диспетчера
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()
    
    # Регистрация роутера
    dp.include_router(router)
    
    try:
        # Отправка тестового сообщения, если указан ID пользователя
        if TELEGRAM_USER_ID:
            try:
                await bot.send_message(chat_id=TELEGRAM_USER_ID, text="Бот запущен и готов к работе!")
                logger.info(f"Отправлено тестовое сообщение пользователю {TELEGRAM_USER_ID}")
            except Exception as e:
                logger.error(f"Ошибка при отправке тестового сообщения: {e}")
        
        # Запуск поллинга
        logger.info("Запуск поллинга бота...")
        await dp.start_polling(bot)
        
    except Exception as e:
        logger.error(f"Ошибка при работе с Telegram API: {e}")
    finally:
        if bot.session:
            await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
   