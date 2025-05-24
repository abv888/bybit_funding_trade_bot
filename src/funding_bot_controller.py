import os
import asyncio
import json
import logging
import sys
import datetime
from dotenv import load_dotenv

# Настройка логирования с правильной кодировкой
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot_controller.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Пути к скриптам
TELEGRAM_BOT_SCRIPT = "telegram_bot.py"
TRADING_BOT_SCRIPT = "funding_arbitrage_bot.py"
STATUS_FILE = "bot_status.json"

async def start_telegram_bot():
    """Запуск Telegram бота в отдельном процессе"""
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable, TELEGRAM_BOT_SCRIPT,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        logger.info(f"Telegram бот запущен с PID {process.pid}")
        return process
    except Exception as e:
        logger.error(f"Ошибка при запуске Telegram бота: {e}")
        return None

async def start_trading_bot():
    """Запуск торгового бота в отдельном процессе"""
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable, TRADING_BOT_SCRIPT,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        logger.info(f"Торговый бот запущен с PID {process.pid}")
        return process
    except Exception as e:
        logger.error(f"Ошибка при запуске торгового бота: {e}")
        return None

async def monitor_processes(telegram_process, trading_process):
    """Мониторинг запущенных процессов"""
    try:
        while True:
            # Проверяем статус процесса Telegram бота
            if telegram_process and telegram_process.returncode is not None:
                logger.error(f"Telegram бот завершился с кодом {telegram_process.returncode}")
                logger.info("Перезапуск Telegram бота...")
                telegram_process = await start_telegram_bot()
            
            # Проверяем статус процесса торгового бота
            if trading_process and trading_process.returncode is not None:
                logger.error(f"Торговый бот завершился с кодом {trading_process.returncode}")
                logger.info("Перезапуск торгового бота...")
                trading_process = await start_trading_bot()
            
            # Сохраняем статус ботов в файл
            status = {
                "telegram_bot": {
                    "running": telegram_process and telegram_process.returncode is None,
                    "pid": telegram_process.pid if telegram_process else None
                },
                "trading_bot": {
                    "running": trading_process and trading_process.returncode is None,
                    "pid": trading_process.pid if trading_process else None
                },
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            with open(STATUS_FILE, "w") as f:
                json.dump(status, f, indent=4)
            
            await asyncio.sleep(10)  # Проверка каждые 10 секунд
    except asyncio.CancelledError:
        # Корректное завершение при остановке
        logger.info("Мониторинг процессов остановлен")
        
        # Завершение процессов
        if telegram_process:
            telegram_process.terminate()
        if trading_process:
            trading_process.terminate()

async def main():
    """Главная функция контроллера ботов"""
    try:
        logger.info("Запуск контроллера ботов для арбитража фандинг рейтов...")
        
        # Запуск Telegram бота
        telegram_process = await start_telegram_bot()
        
        # Даем время Telegram боту инициализироваться
        await asyncio.sleep(5)
        
        # Запуск торгового бота
        trading_process = await start_trading_bot()
        
        # Мониторинг процессов
        monitor_task = asyncio.create_task(monitor_processes(telegram_process, trading_process))
        
        # Ожидаем отмены задачи или Ctrl+C
        try:
            await monitor_task
        except KeyboardInterrupt:
            logger.info("Получен сигнал прерывания, завершаем работу...")
            monitor_task.cancel()
            await asyncio.gather(monitor_task, return_exceptions=True)
        
    except Exception as e:
        logger.error(f"Критическая ошибка в контроллере ботов: {e}")
    finally:
        # Завершение процессов при выходе
        logger.info("Завершение работы контроллера ботов...")
        if 'telegram_process' in locals() and telegram_process:
            telegram_process.terminate()
        if 'trading_process' in locals() and trading_process:
            trading_process.terminate()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Контроллер ботов остановлен пользователем")