import logging
import threading
import json
from typing import Optional, Callable

import telegram
from telegram.ext import Updater, CommandHandler, CallbackContext
from telegram import Update

logger = logging.getLogger(__name__)

class TelegramBot:
    """Класс для работы с Telegram ботом"""
    
    def __init__(self, token: str, user_id: Optional[int] = None):
        """
        Инициализация Telegram бота
        
        Args:
            token: Токен Telegram бота
            user_id: ID пользователя для отправки сообщений
        """
        self.token = token
        self.user_id = user_id
        self.bot = telegram.Bot(token=token)
        
        # Правильная инициализация Updater без передачи token напрямую
        self.updater = Updater(bot=self.bot)
        self.dispatcher = self.updater.dispatcher
        
        # Обработчики по умолчанию
        self.dispatcher.add_handler(CommandHandler("start", self._start_command))
        
        # Сохраняем колбэки для внешних команд
        self.status_callback = None
        self.funding_callback = None
        self.top_callback = None  # Добавлена инициализация для /top
        
    def _start_command(self, update: Update, context: CallbackContext) -> None:
        """Обработчик команды /start"""
        update.message.reply_text(
            "Привет! Я бот для арбитража фандинг рейтов Bybit.\n\n"
            "Доступные команды:\n"
            "/status - Получить текущий статус и баланс\n"
            "/funding - Просмотр ближайших фандинг выплат (по абсолютному значению)\n"
            "/top - Показать топ-10 положительных и топ-10 отрицательных рейтов\n"
        )
    
    def _status_command(self, update: Update, context: CallbackContext) -> None:
        """Обработчик команды /status"""
        if self.status_callback:
            # Сообщаем пользователю, что команда выполняется
            update.message.reply_text("Получение статуса... Пожалуйста, подождите.")
            
            # Запускаем колбэк в отдельном потоке, чтобы не блокировать бота
            threading.Thread(target=self._execute_status_callback, args=(update, context)).start()
        else:
            update.message.reply_text("Команда /status временно недоступна.")
    
    def _execute_status_callback(self, update: Update, context: CallbackContext) -> None:
        """Выполнение колбэка для команды /status"""
        try:
            # Получаем данные от колбэка
            status_text = self.status_callback()
            
            # Отправляем их пользователю
            update.message.reply_text(status_text)
        except Exception as e:
            logger.error(f"Ошибка при выполнении колбэка status: {e}")
            update.message.reply_text("Произошла ошибка при получении статуса.")
    
    def _funding_command(self, update: Update, context: CallbackContext) -> None:
        """Обработчик команды /funding"""
        if self.funding_callback:
            # Сообщаем пользователю, что команда выполняется
            update.message.reply_text("Получение данных о фандинг рейтах... Пожалуйста, подождите.")
            
            # Запускаем колбэк в отдельном потоке
            threading.Thread(target=self._execute_funding_callback, args=(update, context)).start()
        else:
            update.message.reply_text("Команда /funding временно недоступна.")
    
    def _execute_funding_callback(self, update: Update, context: CallbackContext) -> None:
        """Выполнение колбэка для команды /funding"""
        try:
            # Получаем данные от колбэка
            funding_text = self.funding_callback()
            
            # Отправляем их пользователю
            update.message.reply_text(funding_text)
        except Exception as e:
            logger.error(f"Ошибка при выполнении колбэка funding: {e}")
            update.message.reply_text("Произошла ошибка при получении данных о фандинг рейтах.")
    
    def _top_command(self, update: Update, context: CallbackContext) -> None:
        """Обработчик команды /top"""
        if self.top_callback:
            # Сообщаем пользователю, что команда выполняется
            update.message.reply_text("Получение топ фандинг рейтов... Пожалуйста, подождите.")
            
            # Запускаем колбэк в отдельном потоке, чтобы не блокировать бота
            threading.Thread(target=self._execute_top_callback, args=(update, context)).start()
        else:
            update.message.reply_text("Команда /top временно недоступна.")

    def _execute_top_callback(self, update: Update, context: CallbackContext) -> None:
        """Выполнение колбэка для команды /top"""
        try:
            # Получаем данные от колбэка
            top_text = self.top_callback()
            
            # Отправляем их пользователю
            update.message.reply_text(top_text)
        except Exception as e:
            logger.error(f"Ошибка при выполнении колбэка top: {e}")
            update.message.reply_text("Произошла ошибка при получении топ фандинг рейтов.")
    
    def register_status_handler(self, callback: Callable[[], str]) -> None:
        """
        Регистрация обработчика команды /status
        
        Args:
            callback: Функция, возвращающая текст статуса
        """
        self.status_callback = callback
        self.dispatcher.add_handler(CommandHandler("status", self._status_command))
    
    def register_funding_handler(self, callback: Callable[[], str]) -> None:
        """
        Регистрация обработчика команды /funding
        
        Args:
            callback: Функция, возвращающая текст с фандинг рейтами
        """
        self.funding_callback = callback
        self.dispatcher.add_handler(CommandHandler("funding", self._funding_command))
    
    def register_top_handler(self, callback: Callable[[], str]) -> None:
        """
        Регистрация обработчика команды /top
        
        Args:
            callback: Функция, возвращающая текст с топ положительными и отрицательными фандинг рейтами
        """
        self.top_callback = callback
        self.dispatcher.add_handler(CommandHandler("top", self._top_command))
    
    def send_message(self, text: str, chat_id: Optional[int] = None) -> bool:
        """
        Отправка сообщения
        
        Args:
            text: Текст сообщения
            chat_id: ID чата для отправки (если не указан, используется user_id)
            
        Returns:
            bool: Успешность отправки
        """
        try:
            target_id = chat_id or self.user_id
            if target_id:
                self.bot.send_message(
                    chat_id=target_id,
                    text=text,
                    parse_mode="HTML"
                )
                return True
            else:
                logger.warning("Не указан ID пользователя для отправки сообщения")
                return False
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения в Telegram: {e}")
            return False
    
    def start(self) -> None:
        """Запуск бота"""
        logger.info("Запуск Telegram бота...")
        try:
            # Запуск в неблокирующем режиме
            self.updater.start_polling(poll_interval=1.0, timeout=30)
            logger.info("Telegram бот успешно запущен")
        except Exception as e:
            logger.error(f"Ошибка при запуске Telegram бота: {e}")
    
    def stop(self) -> None:
        """Остановка бота"""
        logger.info("Остановка Telegram бота...")
        self.updater.stop()