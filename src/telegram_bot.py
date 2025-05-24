import os
import asyncio
import logging
import json
import datetime
from dotenv import load_dotenv

# Настройка логирования с правильной кодировкой
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("telegram_bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Импорты для бота
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.storage.memory import MemoryStorage

# Конфигурация
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_USER_ID = int(os.getenv("TELEGRAM_USER_ID", "0"))
MIN_FUNDING_RATE = float(os.getenv("MIN_FUNDING_RATE", "0.0001"))

class TelegramBotServer:
    def __init__(self, token, user_id=None):
        self.bot = Bot(token=token)
        self.user_id = user_id
        self.storage = MemoryStorage()
        self.dp = Dispatcher(storage=self.storage)
        self.router = Router()
        self.dp.include_router(self.router)
        
        # Регистрация базовых обработчиков
        self._setup_handlers()
    
    def _setup_handlers(self):
        # Обработчик /start
        @self.router.message(Command("start"))
        async def cmd_start(message: Message):
            await message.answer(
                "Привет! Я бот для арбитража фандинг рейтов Bybit.\n\n"
                "Доступные команды:\n"
                "/status - Получить текущий статус и баланс\n"
                "/funding - Просмотр ближайших фандинг выплат (по абсолютному значению)\n"
                "/top - Показать топ-10 положительных и топ-10 отрицательных рейтов\n"
            )
        
        # Обработчик /status
        @self.router.message(Command("status"))
        async def cmd_status(message: Message):
            try:
                # Попытка прочитать данные из файла состояния
                try:
                    with open("bot_status.json", "r") as f:
                        status_data = json.load(f)

                    telegram_running = status_data.get("telegram_bot", {}).get("running", False)
                    trading_running = status_data.get("trading_bot", {}).get("running", False)
                    update_time = status_data.get("timestamp", "неизвестно")

                    status_text = f"📊 Статус бота (обновлено: {update_time}):\n\n"
                    status_text += f"Telegram бот: {'✅ Активен' if telegram_running else '❌ Неактивен'}\n"
                    status_text += f"Торговый бот: {'✅ Активен' if trading_running else '❌ Неактивен'}\n\n"

                    # Если есть дополнительная информация о балансе и активных сделках
                    if "balance" in status_data:
                        status_text += f"Баланс: {status_data['balance']} USDT\n\n"

                    if "active_trades" in status_data and status_data["active_trades"]:
                        status_text += "Активные сделки:\n\n"
                        for trade_id, trade_data in status_data["active_trades"].items():
                            status_text += (
                                f"Пара: {trade_data['symbol']}\n"
                                f"Направление: {'LONG' if trade_data['side'] == 'Buy' else 'SHORT'}\n"
                                f"Размер: {trade_data['size']}\n"
                                f"Цена входа: {trade_data['entry_price']}\n"
                                f"Время входа: {trade_data['entry_time']}\n\n"
                            )
                    else:
                        status_text += "Нет активных сделок\n\n"

                    if "min_funding_rate" in status_data:
                        status_text += f"Минимальный фандинг для торговли: {status_data['min_funding_rate']*100}%\n"
                    else:
                        status_text += f"Минимальный фандинг для торговли: {MIN_FUNDING_RATE*100}%\n"

                    if "trade_amount_usdt" in status_data:
                        status_text += f"Размер сделки: {status_data['trade_amount_usdt']} USDT\n"

                    if "seconds_before_funding" in status_data:
                        status_text += f"Секунд до фандинга для входа: {status_data['seconds_before_funding']}"

                    await message.answer(status_text)
                    
                except (FileNotFoundError, json.JSONDecodeError):
                    await message.answer(
                        "📊 Статус бота:\n\n"
                        "Информация о статусе недоступна. Торговый бот еще не запущен или не обновлял статус."
                    )
            except Exception as e:
                logger.error(f"Ошибка при выполнении команды /status: {e}")
                await message.answer("Произошла ошибка при получении статуса. Пожалуйста, попробуйте позже.")
        
        # Обработчик /funding
        @self.router.message(Command("funding"))
        async def cmd_funding(message: Message):
            try:
                # Попытка прочитать данные из файла состояния фандинг рейтов
                try:
                    with open("funding_rates.json", "r") as f:
                        funding_data = json.load(f)
                    
                    if funding_data and "top_rates" in funding_data and funding_data["top_rates"]:
                        top_rates = funding_data["top_rates"]
                        update_time = funding_data.get("update_time", "неизвестно")
                        min_rate_percent = funding_data.get("min_funding_rate_percent", MIN_FUNDING_RATE * 100)
                        
                        # Сортируем по абсолютному значению
                        top_rates.sort(key=lambda x: x.get("abs_rate", 0), reverse=True)
                        
                        response = f"🕒 Ближайшие фандинг выплаты (обновлено: {update_time}):\n"
                        response += f"Минимальный рейт для торговли: {min_rate_percent:.5f}%\n\n"
                        
                        # Показываем топ-15 по абсолютному значению
                        for i, rate in enumerate(top_rates[:15], 1):
                            # Определяем знак
                            sign = "+" if rate["rate"] > 0 else ""
                            
                            response += (
                                f"{i}. {rate['symbol']}\n"
                                f"   Рейт: {sign}{rate['rate_percent']:.5f}% ({rate['abs_rate_percent']:.5f}%)\n"
                                f"   Время до: {rate['time_until']}\n"
                                f"   Выплата: {rate['time']}\n\n"
                            )
                        
                        await message.answer(response)
                    else:
                        await message.answer("Информация о фандинг рейтах недоступна или еще не собрана.")
                except (FileNotFoundError, json.JSONDecodeError):
                    await message.answer(
                        "Информация о фандинг рейтах недоступна. Торговый бот еще не запущен или не собрал данные."
                    )
            except Exception as e:
                logger.error(f"Ошибка при выполнении команды /funding: {e}")
                await message.answer("Произошла ошибка при получении фандинг рейтов. Пожалуйста, попробуйте позже.")
        
        # Обработчик /top
        @self.router.message(Command("top"))
        async def cmd_top(message: Message):
            try:
                # Попытка прочитать данные из файла состояния фандинг рейтов
                try:
                    with open("funding_rates.json", "r") as f:
                        funding_data = json.load(f)
                    
                    if funding_data and "top_rates" in funding_data and funding_data["top_rates"]:
                        top_rates = funding_data["top_rates"]
                        update_time = funding_data.get("update_time", "неизвестно")
                        min_rate_percent = funding_data.get("min_funding_rate_percent", MIN_FUNDING_RATE * 100)
                        
                        # Разделяем на положительные и отрицательные
                        positive_rates = [r for r in top_rates if r.get("rate", 0) > 0]
                        negative_rates = [r for r in top_rates if r.get("rate", 0) < 0]
                        
                        # Сортируем по абсолютному значению
                        positive_rates.sort(key=lambda x: x.get("abs_rate", 0), reverse=True)
                        negative_rates.sort(key=lambda x: x.get("abs_rate", 0), reverse=True)
                        
                        response = f"🔝 Топ фандинг рейты (обновлено: {update_time}):\n"
                        response += f"Минимальный рейт для торговли: {min_rate_percent:.5f}%\n\n"
                        
                        # Топ положительные (лонги платят шортам)
                        response += "📈 ТОП ПОЛОЖИТЕЛЬНЫЕ (лонги платят шортам):\n"
                        for i, rate in enumerate(positive_rates[:10], 1):
                            response += (
                                f"{i}. {rate['symbol']}\n"
                                f"   Рейт: +{rate['rate_percent']:.5f}%\n"
                                f"   Время до: {rate['time_until']}\n\n"
                            )
                        
                        # Топ отрицательные (шорты платят лонгам)
                        response += "📉 ТОП ОТРИЦАТЕЛЬНЫЕ (шорты платят лонгам):\n"
                        for i, rate in enumerate(negative_rates[:10], 1):
                            response += (
                                f"{i}. {rate['symbol']}\n"
                                f"   Рейт: {rate['rate_percent']:.5f}%\n"
                                f"   Время до: {rate['time_until']}\n\n"
                            )
                        
                        await message.answer(response)
                    else:
                        await message.answer("Информация о фандинг рейтах недоступна или еще не собрана.")
                except (FileNotFoundError, json.JSONDecodeError) as e:
                    await message.answer(
                        f"Не удалось загрузить данные о фандинг рейтах из файла. Попробуйте позже.\n\nОшибка: {e}"
                    )
            except Exception as e:
                logger.error(f"Ошибка при выполнении команды /top: {e}")
                await message.answer("Произошла ошибка при получении топ фандинг рейтов. Пожалуйста, попробуйте позже.")
    
    async def start(self):
        """Запуск бота"""
        logger.info("Запуск Telegram бота...")
        await self.dp.start_polling(self.bot, skip_updates=True)
    
    async def save_status(self, running=True):
        """Сохранение статуса Telegram бота в файл"""
        try:
            status = {
                "telegram_bot": {
                    "running": running,
                    "start_time": datetime.datetime.now().isoformat() if running else None
                },
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            # Пытаемся обновить существующий файл статуса, если он есть
            try:
                if os.path.exists("bot_status.json"):
                    with open("bot_status.json", "r") as f:
                        existing_status = json.load(f)
                    
                    existing_status["telegram_bot"] = status["telegram_bot"]
                    existing_status["timestamp"] = status["timestamp"]
                    
                    with open("bot_status.json", "w") as f:
                        json.dump(existing_status, f, indent=4)
                else:
                    with open("bot_status.json", "w") as f:
                        json.dump(status, f, indent=4)
                        
                logger.info(f"Сохранен статус Telegram бота (running={running})")
                
            except Exception as e:
                logger.error(f"Ошибка при сохранении статуса: {e}")
                
        except Exception as e:
            logger.error(f"Ошибка при подготовке статуса: {e}")

async def main():
    """Главная функция запуска Telegram бота"""
    try:
        # Проверяем, что токен Telegram бота установлен
        if not TELEGRAM_BOT_TOKEN:
            logger.error("Не установлен токен Telegram бота. Проверьте .env файл")
            return
        
        # Создаем и запускаем бот
        bot_server = TelegramBotServer(TELEGRAM_BOT_TOKEN, TELEGRAM_USER_ID)
        
        # Сохраняем статус запуска
        await bot_server.save_status(running=True)
        
        # Отправляем сообщение о запуске, если указан ID пользователя
        if TELEGRAM_USER_ID:
            try:
                await bot_server.bot.send_message(
                    chat_id=TELEGRAM_USER_ID,
                    text=f"🤖 Telegram бот для арбитража фандинг рейтов запущен!\n\n"
                         f"Доступные команды:\n"
                         f"/status - Получить текущий статус и баланс\n"
                         f"/funding - Просмотр ближайших фандинг выплат\n"
                         f"/top - Показать топ положительных и отрицательных рейтов"
                )
            except Exception as e:
                logger.error(f"Ошибка при отправке стартового сообщения: {e}")
        
        # Запускаем бота
        await bot_server.start()
        
    except Exception as e:
        logger.error(f"Критическая ошибка в Telegram боте: {e}")

if __name__ == "__main__":
    # Обработка Ctrl+C и других сигналов
    import signal
    import sys
    
    def signal_handler(sig, frame):
        logger.info(f"Получен сигнал {sig}, завершаем работу...")
        asyncio.create_task(shutdown())
    
    async def shutdown():
        # Здесь можно добавить код для корректного завершения бота
        sys.exit(0)
    
    # Регистрируем обработчики сигналов
    if sys.platform != 'win32':  # Не работает на Windows
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    # Запускаем основную функцию
    asyncio.run(main())