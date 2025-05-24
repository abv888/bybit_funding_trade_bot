import asyncio
import logging
from typing import Optional, Callable, Dict, Any

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.storage.memory import MemoryStorage

logger = logging.getLogger(__name__)

class TelegramNotifier:
    """Класс для отправки уведомлений через Telegram"""
    
    def __init__(self, token: str, user_id: Optional[int] = None):
        """
        Инициализация Telegram нотификатора
        
        Args:
            token: Токен Telegram бота
            user_id: ID пользователя для отправки сообщений (опционально)
        """
        self.bot = Bot(token=token)
        self.user_id = user_id
        self.storage = MemoryStorage()
        self.dp = Dispatcher(storage=self.storage)
        self.router = Router()
        self.dp.include_router(self.router)
        
        # Словарь для хранения внешних обработчиков команд
        self.command_handlers: Dict[str, Callable] = {}
        
        # Базовые обработчики
        self._setup_base_handlers()
    
    def _setup_base_handlers(self):
        """Настройка базовых обработчиков команд"""
        # Обработчик для команды /start
        @self.router.message(Command("start"))
        async def cmd_start(message: Message):
            await message.answer(
                "Привет! Я бот для арбитража фандинг рейтов Bybit.\n"
                "Используйте /status чтобы получить текущий статус и баланс.\n"
                "Используйте /funding чтобы получить ближайшие фандинг выплаты."
            )
        
        # Динамический обработчик для других команд
        @self.router.message(Command("status", "funding"))
        async def dynamic_command_handler(message: Message):
            command = message.text.split()[0][1:]  # Получаем команду без "/"
            
            if command in self.command_handlers:
                try:
                    await self.command_handlers[command](message)
                except Exception as e:
                    logger.error(f"Ошибка при выполнении команды /{command}: {e}")
                    await message.answer(f"Произошла ошибка при выполнении команды. Пожалуйста, попробуйте позже.")
            else:
                await message.answer(f"Обработчик для команды /{command} еще не зарегистрирован.")
    
    def register_command_handler(self, command: str, handler: Callable):
        """
        Регистрация внешнего обработчика команды
        
        Args:
            command: Название команды (без /)
            handler: Функция-обработчик команды
        """
        self.command_handlers[command] = handler
        logger.info(f"Зарегистрирован обработчик для команды /{command}")
    
    async def send_message(self, text: str, chat_id: Optional[int] = None, **kwargs) -> bool:
        """
        Отправка сообщения
        
        Args:
            text: Текст сообщения
            chat_id: ID чата для отправки (если не указан, используется user_id)
            **kwargs: Дополнительные параметры для API отправки сообщения
            
        Returns:
            bool: Успешность отправки
        """
        try:
            target_id = chat_id or self.user_id
            if target_id:
                await self.bot.send_message(
                    chat_id=target_id,
                    text=text,
                    parse_mode="HTML",
                    **kwargs
                )
                return True
            else:
                logger.warning("Не указан ID пользователя для отправки сообщения")
                return False
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения в Telegram: {e}")
            return False
    
    async def start_polling(self):
        """Запуск бота в отдельном потоке"""
        try:
            # Запуск бота в отдельном потоке
            logger.info("Запуск Telegram бота...")
            await self.dp.start_polling(self.bot, skip_updates=True)
        except Exception as e:
            logger.error(f"Ошибка при запуске Telegram бота: {e}")