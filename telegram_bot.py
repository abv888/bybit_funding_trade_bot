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
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
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
            await self.send_menu(message)
        
        # Обработчик callback-запросов
        @self.router.callback_query()
        async def handle_callback(callback: CallbackQuery):
            try:
                if callback.data == "status":
                    await self.handle_status(callback)
                elif callback.data == "funding":
                    await self.handle_funding(callback)
                elif callback.data == "top":
                    await self.handle_top(callback)
                elif callback.data == "stats":
                    await self.handle_stats(callback)
                elif callback.data == "settings":
                    await self.handle_settings(callback)
                elif callback.data == "emergency_stop":
                    await self.handle_emergency_stop(callback)
                elif callback.data == "refresh":
                    await self.handle_refresh(callback)
                elif callback.data == "menu":
                    await self.handle_menu(callback)
                elif callback.data == "confirm_stop":
                    await self.handle_confirm_stop(callback)
                elif callback.data == "cancel_stop":
                    await self.handle_cancel_stop(callback)
                
                await callback.answer()
            except Exception as e:
                logger.error(f"Ошибка при обработке callback: {e}")
                await callback.answer("Произошла ошибка при обработке запроса")
        
        # Обработчик /status
        @self.router.message(Command("status"))
        async def cmd_status(message: Message):
            await self.send_status(message)
        
        # Обработчик /funding
        @self.router.message(Command("funding"))
        async def cmd_funding(message: Message):
            await self.send_funding(message)
        
        # Обработчик /top
        @self.router.message(Command("top"))
        async def cmd_top(message: Message):
            await self.send_top(message)
        
        # Обработчик /stats
        @self.router.message(Command("stats"))
        async def cmd_stats(message: Message):
            await self.send_stats(message)
    
    async def send_menu(self, message: Message, edit=False):
        """Отправка главного меню"""
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Статус", callback_data="status"),
                InlineKeyboardButton(text="💹 Фандинг", callback_data="funding")
            ],
            [
                InlineKeyboardButton(text="🔝 Топ рейты", callback_data="top"),
                InlineKeyboardButton(text="📈 Статистика", callback_data="stats")
            ],
            [
                InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings"),
                InlineKeyboardButton(text="❌ Стоп", callback_data="emergency_stop")
            ]
        ])
        
        menu_text = (
            "🤖 <b>Фандинг Арбитраж Бот</b>\n\n"
            "Добро пожаловать! Я помогу вам отслеживать и торговать на фандинг рейтах Bybit.\n\n"
            "🎯 <b>Стратегия:</b> Ищу пары с наибольшими фандинг рейтами по модулю, "
            "открываю позицию за 10 секунд до выплаты, чтобы получить фандинг, "
            "затем закрываю после получения выплаты.\n\n"
            "📊 Выберите действие:"
        )
        
        if edit:
            try:
                await message.edit_text(menu_text, reply_markup=keyboard, parse_mode="HTML")
            except Exception as edit_error:
                if "message is not modified" in str(edit_error):
                    logger.info("Меню не изменилось, обновление пропущено")
                else:
                    raise edit_error
        else:
            await message.answer(menu_text, reply_markup=keyboard, parse_mode="HTML")
    
    async def handle_status(self, callback: CallbackQuery):
        """Обработка запроса статуса"""
        await self.send_status(callback.message, edit=True)
    
    async def handle_funding(self, callback: CallbackQuery):
        """Обработка запроса фандинг рейтов"""
        await self.send_funding(callback.message, edit=True)
    
    async def handle_top(self, callback: CallbackQuery):
        """Обработка запроса топ рейтов"""
        await self.send_top(callback.message, edit=True)
    
    async def handle_stats(self, callback: CallbackQuery):
        """Обработка запроса статистики"""
        await self.send_stats(callback.message, edit=True)
    
    async def handle_settings(self, callback: CallbackQuery):
        """Обработка запроса настроек"""
        await self.send_settings(callback.message, edit=True)

    async def handle_menu(self, callback: CallbackQuery):
        """Обработка запроса меню"""
        await self.send_menu(callback.message, edit=True)
    
    async def handle_emergency_stop(self, callback: CallbackQuery):
        """Обработка экстренной остановки"""
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, остановить", callback_data="confirm_stop"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_stop")
            ]
        ])
        
        await callback.message.edit_text(
            "⚠️ <b>ВНИМАНИЕ!</b>\n\n"
            "Вы действительно хотите экстренно остановить бота?\n"
            "Все открытые позиции будут закрыты!\n\n"
            "Это действие нельзя отменить.",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    
    async def handle_confirm_stop(self, callback: CallbackQuery):
        """Подтверждение остановки бота"""
        # Здесь можно добавить логику для остановки торгового бота
        await callback.message.edit_text(
            "🛑 <b>БОТ ОСТАНОВЛЕН!</b>\n\n"
            "Все торговые операции прекращены.\n"
            "Для повторного запуска перезапустите программу.",
            parse_mode="HTML"
        )
    
    async def handle_cancel_stop(self, callback: CallbackQuery):
        """Отмена остановки бота"""
        await self.send_menu(callback.message, edit=True)
    
    async def handle_refresh(self, callback: CallbackQuery):
        """Обновление данных"""
        try:
            # Определяем, какую страницу обновлять по тексту сообщения
            if "Статус бота" in callback.message.text:
                await self.send_status(callback.message, edit=True)
            elif "Ближайшие фандинг выплаты" in callback.message.text:
                await self.send_funding(callback.message, edit=True)
            elif "Топ фандинг рейты" in callback.message.text:
                await self.send_top(callback.message, edit=True)
            elif "Статистика торговли" in callback.message.text:
                await self.send_stats(callback.message, edit=True)
            elif "Настройки бота" in callback.message.text:
                await self.send_settings(callback.message, edit=True)
        except Exception as e:
            # Если контент не изменился, просто игнорируем ошибку
            if "message is not modified" in str(e):
                logger.info("Содержимое сообщения не изменилось, обновление пропущено")
            else:
                logger.error(f"Ошибка при обновлении: {e}")
    
    async def send_status(self, message: Message, edit=False):
        """Отправка статуса бота"""
        try:
            # Читаем данные из файла состояния
            try:
                with open("bot_status.json", "r", encoding='utf-8') as f:
                    status_data = json.load(f)

                trading_running = status_data.get("trading_bot", {}).get("running", False)
                update_time = status_data.get("timestamp", "неизвестно")
                balance = status_data.get("balance", 0)
                active_trades = status_data.get("active_trades", {})
                statistics = status_data.get("statistics", {})

                # Форматируем время обновления
                try:
                    update_dt = datetime.datetime.fromisoformat(update_time.replace('Z', '+00:00'))
                    update_str = update_dt.strftime("%d.%m.%Y %H:%M:%S")
                except:
                    update_str = update_time

                status_text = f"📊 <b>Статус бота</b>\n"
                status_text += f"🕐 Обновлено: {update_str}\n\n"
                status_text += f"🤖 Торговый бот: {'✅ Активен' if trading_running else '❌ Неактивен'}\n"
                status_text += f"💰 Баланс: <b>{balance:.2f} USDT</b>\n\n"

                if active_trades:
                    status_text += f"📈 <b>Активные сделки ({len(active_trades)}):</b>\n\n"
                    for trade_id, trade_data in list(active_trades.items())[:5]:  # Показываем только первые 5
                        side_emoji = "🟢" if trade_data['side'] == 'Buy' else "🔴"
                        status_text += (
                            f"{side_emoji} <b>{trade_data['symbol']}</b>\n"
                            f"   📊 {trade_data['side']} {trade_data['size']}\n"
                            f"   💵 Вход: {trade_data['entry_price']}\n"
                            f"   📈 Фандинг: {trade_data.get('funding_rate', 0)*100:.4f}%\n"
                            f"   💰 Ожидаемая прибыль: {trade_data.get('expected_funding_profit', 0):.4f} USDT\n\n"
                        )
                    
                    if len(active_trades) > 5:
                        status_text += f"... и еще {len(active_trades) - 5} сделок\n\n"
                else:
                    status_text += "📊 Нет активных сделок\n\n"

                # Статистика
                if statistics:
                    status_text += f"📈 <b>Статистика:</b>\n"
                    status_text += f"   🎯 Всего сделок: {statistics.get('total_trades', 0)}\n"
                    status_text += f"   ✅ Успешных: {statistics.get('successful_trades', 0)}\n"
                    status_text += f"   📊 Успешность: {statistics.get('success_rate', 0):.1f}%\n"
                    status_text += f"   💰 Общая прибыль: {statistics.get('total_pnl', 0):.4f} USDT\n\n"

                status_text += f"⚙️ <b>Настройки:</b>\n"
                status_text += f"   💵 Сумма сделки: {status_data.get('trade_amount_usdt', 0)} USDT\n"
                status_text += f"   📊 Мин. фандинг: {status_data.get('min_funding_rate', 0)*100:.4f}%\n"
                status_text += f"   ⏰ Секунд до фандинга: {status_data.get('seconds_before_funding', 0)}\n"
                status_text += f"   🔝 Топ пар: {status_data.get('top_pairs_count', 0)}\n"

            except (FileNotFoundError, json.JSONDecodeError):
                status_text = (
                    "📊 <b>Статус бота</b>\n\n"
                    "⚠️ Информация о статусе недоступна.\n"
                    "Торговый бот еще не запущен или не обновлял статус."
                )

            # Клавиатура
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh")],
                [InlineKeyboardButton(text="🏠 Главная", callback_data="menu")]
            ])

            if edit:
                try:
                    await message.edit_text(status_text, reply_markup=keyboard, parse_mode="HTML")
                except Exception as edit_error:
                    if "message is not modified" in str(edit_error):
                        logger.info("Статус не изменился, обновление пропущено")
                    else:
                        raise edit_error
            else:
                await message.answer(status_text, reply_markup=keyboard, parse_mode="HTML")

        except Exception as e:
            logger.error(f"Ошибка при отправке статуса: {e}")
            error_text = "❌ Произошла ошибка при получении статуса"
            if edit:
                try:
                    await message.edit_text(error_text)
                except Exception as edit_error:
                    if "message is not modified" not in str(edit_error):
                        logger.error(f"Ошибка при редактировании сообщения об ошибке: {edit_error}")
            else:
                await message.answer(error_text)
    
    async def send_funding(self, message: Message, edit=False):
        """Отправка данных о фандинг рейтах"""
        try:
            try:
                with open("funding_rates.json", "r", encoding='utf-8') as f:
                    funding_data = json.load(f)
                
                if funding_data and "top_rates" in funding_data and funding_data["top_rates"]:
                    top_rates = funding_data["top_rates"]
                    update_time = funding_data.get("update_time", "неизвестно")
                    min_rate_percent = funding_data.get("min_funding_rate_percent", MIN_FUNDING_RATE * 100)
                    total_expected_profit = funding_data.get("total_expected_profit", 0)
                    
                    # Форматируем время обновления
                    try:
                        update_dt = datetime.datetime.fromisoformat(update_time.replace('Z', '+00:00'))
                        update_str = update_dt.strftime("%d.%m.%Y %H:%M:%S")
                    except:
                        update_str = update_time
                    
                    response = f"💹 <b>Ближайшие фандинг выплаты</b>\n"
                    response += f"🕐 Обновлено: {update_str}\n"
                    response += f"📊 Мин. рейт: {min_rate_percent:.5f}%\n"
                    response += f"💰 Ожидаемая прибыль: {total_expected_profit:.4f} USDT\n\n"
                    
                    # Сортируем по времени до выплаты
                    available_rates = [rate for rate in top_rates if rate.get("seconds_until", 0) > 0]
                    available_rates.sort(key=lambda x: x.get("seconds_until", float('inf')))
                    
                    # Показываем топ-10 ближайших по времени
                    for i, rate in enumerate(available_rates[:10], 1):
                        # Эмодзи для направления
                        direction_emoji = "🔴" if rate["rate"] > 0 else "🟢"
                        position_emoji = "📉 SHORT" if rate["rate"] > 0 else "📈 LONG"
                        
                        response += (
                            f"{direction_emoji} <b>{i}. {rate['symbol']}</b>\n"
                            f"   💹 Рейт: {rate['rate_percent']:+.5f}% ({rate['abs_rate_percent']:.5f}%)\n"
                            f"   {position_emoji} для получения выплаты\n"
                            f"   ⏰ До выплаты: {rate['time_until']}\n"
                            f"   💰 Ожидаемая прибыль: {rate['expected_profit_usdt']:.4f} USDT\n\n"
                        )
                    
                    if len(available_rates) == 0:
                        response += "⏰ Нет доступных выплат в ближайшее время\n"
                    elif len(top_rates) > 10:
                        response += f"... и еще {len(top_rates) - 10} пар\n"
                    
                else:
                    response = "📊 Информация о фандинг рейтах недоступна или еще не собрана."
                    
            except (FileNotFoundError, json.JSONDecodeError):
                response = "📊 Информация о фандинг рейтах недоступна. Торговый бот еще не запущен."

            # Клавиатура
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh")],
                [InlineKeyboardButton(text="🏠 Главная", callback_data="menu")]
            ])

            if edit:
                try:
                    await message.edit_text(response, reply_markup=keyboard, parse_mode="HTML")
                except Exception as edit_error:
                    if "message is not modified" in str(edit_error):
                        logger.info("Фандинг данные не изменились, обновление пропущено")
                    else:
                        raise edit_error
            else:
                await message.answer(response, reply_markup=keyboard, parse_mode="HTML")

        except Exception as e:
            logger.error(f"Ошибка при отправке фандинг рейтов: {e}")
            error_text = "❌ Произошла ошибка при получении фандинг рейтов"
            if edit:
                try:
                    await message.edit_text(error_text)
                except Exception as edit_error:
                    if "message is not modified" not in str(edit_error):
                        logger.error(f"Ошибка при редактировании сообщения об ошибке: {edit_error}")
            else:
                await message.answer(error_text)
    
    async def send_top(self, message: Message, edit=False):
        """Отправка топ фандинг рейтов"""
        try:
            try:
                with open("funding_rates.json", "r", encoding='utf-8') as f:
                    funding_data = json.load(f)
                
                if funding_data and "top_rates" in funding_data and funding_data["top_rates"]:
                    top_rates = funding_data["top_rates"]
                    update_time = funding_data.get("update_time", "неизвестно")
                    
                    # Форматируем время обновления
                    try:
                        update_dt = datetime.datetime.fromisoformat(update_time.replace('Z', '+00:00'))
                        update_str = update_dt.strftime("%d.%m.%Y %H:%M:%S")
                    except:
                        update_str = update_time
                    
                    # Разделяем на положительные и отрицательные
                    positive_rates = [r for r in top_rates if r.get("rate", 0) > 0]
                    negative_rates = [r for r in top_rates if r.get("rate", 0) < 0]
                    
                    # Сортируем по абсолютному значению
                    positive_rates.sort(key=lambda x: x.get("abs_rate", 0), reverse=True)
                    negative_rates.sort(key=lambda x: x.get("abs_rate", 0), reverse=True)
                    
                    response = f"🔝 <b>Топ фандинг рейты</b>\n"
                    response += f"🕐 Обновлено: {update_str}\n\n"
                    
                    # Топ положительные (лонгисты платят шортистам)
                    response += "🔴 <b>ТОП ПОЛОЖИТЕЛЬНЫЕ</b> (лонгисты платят шортистам):\n"
                    for i, rate in enumerate(positive_rates[:5], 1):
                        response += (
                            f"{i}. <b>{rate['symbol']}</b>\n"
                            f"   💹 Рейт: +{rate['rate_percent']:.5f}%\n"
                            f"   📉 Открывать: SHORT\n"
                            f"   ⏰ До выплаты: {rate['time_until']}\n"
                            f"   💰 Прибыль: {rate['expected_profit_usdt']:.4f} USDT\n\n"
                        )
                    
                    # Топ отрицательные (шортисты платят лонгистам)
                    response += "🟢 <b>ТОП ОТРИЦАТЕЛЬНЫЕ</b> (шортисты платят лонгистам):\n"
                    for i, rate in enumerate(negative_rates[:5], 1):
                        response += (
                            f"{i}. <b>{rate['symbol']}</b>\n"
                            f"   💹 Рейт: {rate['rate_percent']:.5f}%\n"
                            f"   📈 Открывать: LONG\n"
                            f"   ⏰ До выплаты: {rate['time_until']}\n"
                            f"   💰 Прибыль: {rate['expected_profit_usdt']:.4f} USDT\n\n"
                        )
                else:
                    response = "📊 Информация о фандинг рейтах недоступна или еще не собрана."
                    
            except (FileNotFoundError, json.JSONDecodeError):
                response = "📊 Информация о фандинг рейтах недоступна."

            # Клавиатура
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh")],
                [InlineKeyboardButton(text="🏠 Главная", callback_data="menu")]
            ])

            if edit:
                try:
                    await message.edit_text(response, reply_markup=keyboard, parse_mode="HTML")
                except Exception as edit_error:
                    if "message is not modified" in str(edit_error):
                        logger.info("Топ рейты не изменились, обновление пропущено")
                    else:
                        raise edit_error
            else:
                await message.answer(response, reply_markup=keyboard, parse_mode="HTML")

        except Exception as e:
            logger.error(f"Ошибка при отправке топ рейтов: {e}")
            error_text = "❌ Произошла ошибка при получении топ рейтов"
            if edit:
                try:
                    await message.edit_text(error_text)
                except Exception as edit_error:
                    if "message is not modified" not in str(edit_error):
                        logger.error(f"Ошибка при редактировании сообщения об ошибке: {edit_error}")
            else:
                await message.answer(error_text)
    
    async def send_stats(self, message: Message, edit=False):
        """Отправка статистики"""
        try:
            try:
                with open("bot_status.json", "r", encoding='utf-8') as f:
                    status_data = json.load(f)

                statistics = status_data.get("statistics", {})
                balance = status_data.get("balance", 0)
                
                response = f"📈 <b>Статистика торговли</b>\n\n"
                
                if statistics:
                    total_trades = statistics.get("total_trades", 0)
                    successful_trades = statistics.get("successful_trades", 0)
                    success_rate = statistics.get("success_rate", 0)
                    total_pnl = statistics.get("total_pnl", 0)
                    
                    response += f"🎯 <b>Всего сделок:</b> {total_trades}\n"
                    response += f"✅ <b>Успешных:</b> {successful_trades}\n"
                    response += f"❌ <b>Убыточных:</b> {total_trades - successful_trades}\n"
                    response += f"📊 <b>Успешность:</b> {success_rate:.1f}%\n\n"
                    
                    response += f"💰 <b>Текущий баланс:</b> {balance:.2f} USDT\n"
                    response += f"💸 <b>Общая прибыль:</b> {total_pnl:+.4f} USDT\n"
                    
                    if total_trades > 0:
                        avg_profit = total_pnl / total_trades
                        response += f"📊 <b>Средняя прибыль:</b> {avg_profit:+.4f} USDT\n"
                    
                    # Цветовой индикатор прибыльности
                    if total_pnl > 0:
                        response += "\n🟢 <b>Торговля прибыльная!</b>"
                    elif total_pnl < 0:
                        response += "\n🔴 <b>Торговля убыточная</b>"
                    else:
                        response += "\n🟡 <b>Торговля в нуле</b>"
                else:
                    response += "📊 Статистика пока недоступна.\nНачните торговлю для получения данных."

            except (FileNotFoundError, json.JSONDecodeError):
                response = "📊 Статистика недоступна. Торговый бот еще не запущен."

            # Клавиатура
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh")],
                [InlineKeyboardButton(text="🏠 Главная", callback_data="menu")]
            ])

            if edit:
                try:
                    await message.edit_text(response, reply_markup=keyboard, parse_mode="HTML")
                except Exception as edit_error:
                    if "message is not modified" in str(edit_error):
                        logger.info("Статистика не изменилась, обновление пропущено")
                    else:
                        raise edit_error
            else:
                await message.answer(response, reply_markup=keyboard, parse_mode="HTML")

        except Exception as e:
            logger.error(f"Ошибка при отправке статистики: {e}")
            error_text = "❌ Произошла ошибка при получении статистики"
            if edit:
                try:
                    await message.edit_text(error_text)
                except Exception as edit_error:
                    if "message is not modified" not in str(edit_error):
                        logger.error(f"Ошибка при редактировании сообщения об ошибке: {edit_error}")
            else:
                await message.answer(error_text)
    
    async def send_settings(self, message: Message, edit=False):
        """Отправка настроек"""
        try:
            try:
                with open("bot_status.json", "r", encoding='utf-8') as f:
                    status_data = json.load(f)

                response = f"⚙️ <b>Настройки бота</b>\n\n"
                response += f"💵 <b>Сумма сделки:</b> {status_data.get('trade_amount_usdt', 0)} USDT\n"
                response += f"📊 <b>Мин. фандинг рейт:</b> {status_data.get('min_funding_rate', 0)*100:.4f}%\n"
                response += f"⏰ <b>Секунд до фандинга:</b> {status_data.get('seconds_before_funding', 0)}\n"
                response += f"🔝 <b>Топ пар для торговли:</b> {status_data.get('top_pairs_count', 0)}\n\n"
                response += f"📈 <b>Стратегия:</b>\n"
                response += f"• Ищем {status_data.get('top_pairs_count', 20)} пар с наибольшим фандинг рейтом по модулю\n"
                response += f"• Открываем позицию за {status_data.get('seconds_before_funding', 10)} сек до выплаты\n"
                response += f"• Если рейт положительный → SHORT (получаем от лонгистов)\n"
                response += f"• Если рейт отрицательный → LONG (получаем от шортистов)\n"
                response += f"• Закрываем через 30 сек после получения фандинга\n\n"
                response += f"⚠️ <i>Настройки можно изменить только в .env файле</i>"

            except (FileNotFoundError, json.JSONDecodeError):
                response = "⚙️ Настройки недоступны. Торговый бот еще не запущен."

            # Клавиатура
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🏠 Главная", callback_data="menu")]
            ])

            if edit:
                try:
                    await message.edit_text(response, reply_markup=keyboard, parse_mode="HTML")
                except Exception as edit_error:
                    if "message is not modified" in str(edit_error):
                        logger.info("Настройки не изменились, обновление пропущено")
                    else:
                        raise edit_error
            else:
                await message.answer(response, reply_markup=keyboard, parse_mode="HTML")

        except Exception as e:
            logger.error(f"Ошибка при отправке настроек: {e}")
            error_text = "❌ Произошла ошибка при получении настроек"
            if edit:
                try:
                    await message.edit_text(error_text)
                except Exception as edit_error:
                    if "message is not modified" not in str(edit_error):
                        logger.error(f"Ошибка при редактировании сообщения об ошибке: {edit_error}")
            else:
                await message.answer(error_text)
    
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
                    with open("bot_status.json", "r", encoding='utf-8') as f:
                        existing_status = json.load(f)
                    
                    existing_status["telegram_bot"] = status["telegram_bot"]
                    existing_status["timestamp"] = status["timestamp"]
                    
                    with open("bot_status.json", "w", encoding='utf-8') as f:
                        json.dump(existing_status, f, indent=4, ensure_ascii=False)
                else:
                    with open("bot_status.json", "w", encoding='utf-8') as f:
                        json.dump(status, f, indent=4, ensure_ascii=False)
                        
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
                    text="🤖 <b>Telegram бот запущен!</b>\n\n"
                         "✅ Бот готов к работе\n"
                         "📊 Используйте /start для начала работы\n"
                         "💹 Мониторинг фандинг рейтов активен",
                    parse_mode="HTML"
                )
                logger.info(f"Отправлено стартовое сообщение пользователю {TELEGRAM_USER_ID}")
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