import os
import asyncio
import datetime
import json
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
import signal
import sys

import pandas as pd
from dotenv import load_dotenv

from bybit_client import BybitClient

# Настройка логирования с правильной кодировкой
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_USER_ID = int(os.getenv("TELEGRAM_USER_ID", "0"))
TRADE_AMOUNT_USDT = float(os.getenv("TRADE_AMOUNT_USDT", "10"))
MIN_FUNDING_RATE = float(os.getenv("MIN_FUNDING_RATE", "0.0001"))  # Минимальный фандинг рейт для торговли (0.01%)
SECONDS_BEFORE_FUNDING = int(os.getenv("SECONDS_BEFORE_FUNDING", "10"))  # За сколько секунд до выплаты открывать позицию
TOP_PAIRS_COUNT = int(os.getenv("TOP_PAIRS_COUNT", "20"))  # Количество топ пар по модулю рейта
SECONDS_AFTER_FUNDING_TO_CLOSE = int(os.getenv("SECONDS_AFTER_FUNDING_TO_CLOSE", "30"))  # Через сколько секунд после фандинга закрывать

# Печатаем скрытую информацию для отладки (API ключи скрыты)
logger.info(f"API ключ Bybit: {BYBIT_API_KEY[:5]}... (скрыт)")
logger.info(f"Токен Telegram бота: {TELEGRAM_BOT_TOKEN[:5]}... (скрыт)")
logger.info(f"ID пользователя Telegram: {TELEGRAM_USER_ID}")

class TradingBot:
    def __init__(self):
        """Инициализация бота"""
        self.bybit = BybitClient(
            api_key=BYBIT_API_KEY,
            api_secret=BYBIT_API_SECRET,
            testnet=False
        )
        
        self.balance = 0.0
        self.active_trades = {}
        self.funding_schedule = {}
        self.should_run = True
        self.telegram_bot = None
        
        # Статистика
        self.total_trades = 0
        self.successful_trades = 0
        self.total_pnl = 0.0
        
        logger.info("Инициализация торгового бота завершена")
    
    async def init_telegram_notifications(self):
        """Инициализация Telegram уведомлений"""
        if TELEGRAM_BOT_TOKEN and TELEGRAM_USER_ID:
            try:
                from aiogram import Bot
                self.telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)
                await self.send_telegram_message("🚀 Фандинг арбитраж бот запущен!")
                logger.info("Telegram уведомления инициализированы")
            except Exception as e:
                logger.error(f"Ошибка инициализации Telegram: {e}")
    
    async def send_telegram_message(self, message: str):
        """Отправка сообщения в Telegram"""
        if self.telegram_bot and TELEGRAM_USER_ID:
            try:
                await self.telegram_bot.send_message(chat_id=TELEGRAM_USER_ID, text=message)
            except Exception as e:
                logger.error(f"Ошибка отправки Telegram сообщения: {e}")
    
    async def update_balance(self):
        """Обновление текущего баланса USDT"""
        try:
            self.balance = await self.bybit.get_wallet_balance("USDT")
            self.save_status()
            return self.balance
        except Exception as e:
            logger.error(f"Ошибка при обновлении баланса: {e}")
            return self.balance
    
    async def get_funding_rates(self) -> pd.DataFrame:
        """Получение и обновление фандинг рейтов"""
        try:
            funding_df = await self.bybit.get_funding_rates()
            
            if not funding_df.empty:
                # Обновляем глобальное расписание
                self.funding_schedule = {
                    row["symbol"]: {
                        "nextFundingTime": row["nextFundingTime"],
                        "predictedRate": row["predictedRate"],
                        "timestamp": row["timestamp"]
                    } 
                    for _, row in funding_df.iterrows()
                }
                
                logger.info(f"Обновлено расписание фандинг выплат для {len(funding_df)} пар")
                
                # Сохраняем данные в файл
                await self.save_funding_rates(funding_df)
                
            return funding_df
        except Exception as e:
            logger.error(f"Ошибка при получении фандинг рейтов: {e}")
            return pd.DataFrame()
    
    def save_status(self):
        """Сохранение текущего статуса бота в файл"""
        try:
            # Подготовка данных статуса
            status_data = {
                "trading_bot": {
                    "running": True,
                    "start_time": datetime.datetime.now().isoformat()
                },
                "balance": self.balance,
                "active_trades": {k: self.serialize_trade_data(v) for k, v in self.active_trades.items()},
                "min_funding_rate": MIN_FUNDING_RATE,
                "trade_amount_usdt": TRADE_AMOUNT_USDT,
                "seconds_before_funding": SECONDS_BEFORE_FUNDING,
                "top_pairs_count": TOP_PAIRS_COUNT,
                "statistics": {
                    "total_trades": self.total_trades,
                    "successful_trades": self.successful_trades,
                    "success_rate": (self.successful_trades / max(self.total_trades, 1)) * 100,
                    "total_pnl": self.total_pnl
                },
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            # Сохраняем в файл
            with open("bot_status.json", "w", encoding='utf-8') as f:
                json.dump(status_data, f, indent=4, default=self.json_serial, ensure_ascii=False)
            
            logger.info("Сохранен статус торгового бота")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении статуса бота: {e}")
    
    async def save_funding_rates(self, funding_df: pd.DataFrame):
        """Сохранение данных о фандинг рейтах в файл"""
        try:
            if funding_df.empty:
                return
            
            # Добавляем колонку с абсолютным значением фандинг рейта
            funding_df["abs_rate"] = funding_df["predictedRate"].abs()
            
            # Фильтруем только те пары, у которых фандинг рейт больше минимального
            filtered_df = funding_df[funding_df["abs_rate"] >= MIN_FUNDING_RATE]
            
            if filtered_df.empty:
                logger.info(f"Нет пар с фандинг рейтом >= {MIN_FUNDING_RATE}")
                
                # Создаем пустой файл с метаданными
                funding_data = {
                    "top_rates": [],
                    "update_time": datetime.datetime.now().isoformat(),
                    "total_pairs": len(funding_df),
                    "filtered_pairs": 0,
                    "min_funding_rate": MIN_FUNDING_RATE,
                    "min_funding_rate_percent": MIN_FUNDING_RATE * 100
                }
                
                with open("funding_rates.json", "w", encoding='utf-8') as f:
                    json.dump(funding_data, f, indent=4, default=self.json_serial, ensure_ascii=False)
                
                return
            
            # Получаем текущее время для расчета времени до выплаты
            current_time = datetime.datetime.now().timestamp()
            
            # Сортируем по абсолютному значению фандинг рейта (берем ТОП-20 по модулю)
            top_pairs_df = filtered_df.nlargest(TOP_PAIRS_COUNT, 'abs_rate')
            
            # Список для сохранения данных о топ рейтах
            top_rates = []
            
            # Проходим по всем топовым парам
            for _, row in top_pairs_df.iterrows():
                symbol = row["symbol"]
                next_funding_time = row["nextFundingTime"]
                predicted_rate = row["predictedRate"]
                abs_rate = row["abs_rate"]
                time_until = next_funding_time - current_time
                
                # Форматируем время до выплаты для вывода
                if time_until > 0:
                    hours, remainder = divmod(time_until, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    time_until_str = f"{int(hours)}ч {int(minutes)}м {int(seconds)}с"
                else:
                    time_until_str = "Прошло"
                
                # Определяем направление фандинг рейта
                direction = "positive" if predicted_rate > 0 else "negative"
                
                # Определяем, какую позицию нужно открыть для получения выплаты
                position_to_open = "SHORT" if predicted_rate > 0 else "LONG"
                
                # Добавляем в список для сохранения
                top_rates.append({
                    "symbol": symbol,
                    "rate": float(predicted_rate),
                    "rate_percent": float(predicted_rate) * 100,  # В процентах
                    "abs_rate": float(abs_rate),
                    "abs_rate_percent": float(abs_rate) * 100,  # Абсолютное значение в процентах
                    "time_until": time_until_str,
                    "time": row["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                    "next_funding_time": next_funding_time,
                    "seconds_until": time_until,
                    "direction": direction,
                    "position_to_open": position_to_open,
                    "expected_profit_usdt": float(abs_rate) * TRADE_AMOUNT_USDT  # Ожидаемая прибыль в USDT
                })
            
            # Сохраняем топ рейты в файл
            funding_data = {
                "top_rates": top_rates,
                "update_time": datetime.datetime.now().isoformat(),
                "total_pairs": len(funding_df),
                "filtered_pairs": len(filtered_df),
                "top_pairs_count": len(top_rates),
                "min_funding_rate": MIN_FUNDING_RATE,
                "min_funding_rate_percent": MIN_FUNDING_RATE * 100,
                "trade_amount_usdt": TRADE_AMOUNT_USDT,
                "total_expected_profit": sum(rate["expected_profit_usdt"] for rate in top_rates)
            }
            
            with open("funding_rates.json", "w", encoding='utf-8') as f:
                json.dump(funding_data, f, indent=4, default=self.json_serial, ensure_ascii=False)
            
            logger.info(f"Сохранено {len(top_rates)} топовых пар в funding_rates.json")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных о фандинг рейтах: {e}")
    
    @staticmethod
    def json_serial(obj):
        """Функция для сериализации объектов datetime в JSON"""
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
    
    @staticmethod
    def serialize_trade_data(trade_data):
        """Подготовка данных о сделке для сохранения в JSON"""
        serialized = dict(trade_data)
        
        # Преобразуем datetime объекты в строки
        if "entry_time" in serialized and isinstance(serialized["entry_time"], datetime.datetime):
            serialized["entry_time"] = serialized["entry_time"].isoformat()
            
        return serialized
    
    async def open_position(self, symbol: str, side: str, size: float, funding_rate: float) -> Optional[str]:
        """Открытие позиции"""
        try:
            # Получаем текущую цену
            ticker = await self.bybit.get_ticker(symbol)
            if not ticker:
                logger.error(f"Не удалось получить тикер для {symbol}")
                return None
                
            price = float(ticker["lastPrice"])
            
            # Размещаем рыночный ордер
            order = await self.bybit.place_market_order(symbol, side, size)
            
            if order:
                order_id = order["orderId"]
                self.active_trades[order_id] = {
                    "symbol": symbol,
                    "side": side,
                    "size": size,
                    "entry_price": price,
                    "entry_time": datetime.datetime.now(),
                    "funding_rate": funding_rate,
                    "expected_funding_profit": abs(funding_rate) * price * size,
                    "funding_collected": False,
                    "closed": False
                }
                
                logger.info(f"✅ Открыта позиция: {symbol} {side} {size} по цене {price}")
                logger.info(f"💰 Ожидаемая прибыль от фандинга: {abs(funding_rate) * price * size:.4f} USDT")
                
                # Отправляем уведомление в Telegram
                await self.send_telegram_message(
                    f"✅ Открыта позиция:\n"
                    f"📈 {symbol} {side} {size}\n"
                    f"💵 Цена входа: {price}\n"
                    f"📊 Фандинг рейт: {funding_rate*100:.4f}%\n"
                    f"💰 Ожидаемая прибыль: {abs(funding_rate) * price * size:.4f} USDT"
                )
                
                # Обновляем статус
                self.save_status()
                
                return order_id
            else:
                logger.error(f"Не удалось открыть позицию для {symbol}")
                return None
                
        except Exception as e:
            logger.error(f"Исключение при открытии позиции {symbol}: {e}")
            return None
    
    async def close_position(self, order_id: str) -> bool:
        """Закрытие позиции"""
        try:
            if order_id not in self.active_trades:
                logger.error(f"Сделка {order_id} не найдена в активных")
                return False
                
            trade_data = self.active_trades[order_id]
            symbol = trade_data["symbol"]
            side = trade_data["side"]
            size = trade_data["size"]
            entry_price = trade_data["entry_price"]
            
            # Противоположная сторона для закрытия
            close_side = "Sell" if side == "Buy" else "Buy"
            
            # Получаем текущую цену
            ticker = await self.bybit.get_ticker(symbol)
            if not ticker:
                logger.error(f"Не удалось получить тикер для {symbol} при закрытии")
                return False
                
            exit_price = float(ticker["lastPrice"])
            
            # Закрываем позицию рыночным ордером
            order = await self.bybit.place_market_order(symbol, close_side, size, reduce_only=True)
            
            if order:
                # Рассчитываем прибыль/убыток от изменения цены
                if side == "Buy":  # Был лонг, теперь продаем
                    price_pnl = (exit_price - entry_price) * size
                else:  # Был шорт, теперь покупаем
                    price_pnl = (entry_price - exit_price) * size
                
                # Прибыль от фандинга (уже собрана)
                funding_pnl = trade_data.get("expected_funding_profit", 0)
                
                total_pnl = price_pnl + funding_pnl
                
                # Обновляем статистику
                self.total_trades += 1
                if total_pnl > 0:
                    self.successful_trades += 1
                self.total_pnl += total_pnl
                
                # Помечаем сделку как закрытую
                trade_data["closed"] = True
                trade_data["exit_price"] = exit_price
                trade_data["exit_time"] = datetime.datetime.now()
                trade_data["price_pnl"] = price_pnl
                trade_data["total_pnl"] = total_pnl
                
                logger.info(f"✅ Закрыта позиция: {symbol} {close_side} {size} по цене {exit_price}")
                logger.info(f"💰 Прибыль от цены: {price_pnl:.4f} USDT")
                logger.info(f"💰 Прибыль от фандинга: {funding_pnl:.4f} USDT")
                logger.info(f"💰 Общая прибыль: {total_pnl:.4f} USDT")
                
                # Отправляем уведомление в Telegram
                await self.send_telegram_message(
                    f"✅ Закрыта позиция:\n"
                    f"📈 {symbol} {close_side} {size}\n"
                    f"💵 Цена выхода: {exit_price}\n"
                    f"📊 Прибыль от цены: {price_pnl:.4f} USDT\n"
                    f"💰 Прибыль от фандинга: {funding_pnl:.4f} USDT\n"
                    f"🎯 Общая прибыль: {total_pnl:.4f} USDT"
                )
                
                # Обновляем баланс
                await self.update_balance()
                
                # Удаляем из активных сделок
                del self.active_trades[order_id]
                
                # Обновляем статус
                self.save_status()
                
                return True
            else:
                logger.error(f"Не удалось закрыть позицию для {symbol}")
                return False
                
        except Exception as e:
            logger.error(f"Исключение при закрытии позиции {order_id}: {e}")
            return False
    
    async def monitor_funding_rates(self):
        """Основной цикл мониторинга и торговли"""
        logger.info("🚀 Запущен мониторинг фандинг рейтов")
        
        while self.should_run:
            try:
                # Обновляем информацию о фандинг рейтах
                funding_df = await self.get_funding_rates()
                
                if not funding_df.empty:
                    current_time = datetime.datetime.now().timestamp()
                    
                    # Добавляем колонку с абсолютным значением фандинг рейта
                    funding_df["abs_rate"] = funding_df["predictedRate"].abs()
                    
                    # Фильтруем только те пары, у которых фандинг рейт больше минимального
                    filtered_df = funding_df[funding_df["abs_rate"] >= MIN_FUNDING_RATE]
                    
                    if not filtered_df.empty:
                        # Берем ТОП-20 пар по абсолютному значению фандинг рейта
                        top_pairs_df = filtered_df.nlargest(TOP_PAIRS_COUNT, 'abs_rate')
                        
                        # Мониторим время для открытия позиций
                        for _, row in top_pairs_df.iterrows():
                            symbol = row["symbol"]
                            next_funding_time = row["nextFundingTime"]
                            predicted_rate = row["predictedRate"]
                            abs_rate = row["abs_rate"]
                            time_until = next_funding_time - current_time
                            
                            # Проверяем, подходит ли время для открытия позиции
                            if 0 < time_until <= SECONDS_BEFORE_FUNDING:
                                # Определяем сторону для позиции (противоположную для получения выплаты)
                                side = "Sell" if predicted_rate > 0 else "Buy"
                                
                                # Проверяем, что у нас нет открытой позиции по этому символу
                                symbol_active = any(
                                    trade_data["symbol"] == symbol and not trade_data.get("closed", False)
                                    for trade_data in self.active_trades.values()
                                )
                                    
                                if not symbol_active:
                                    # Рассчитываем размер позиции
                                    size = await self.bybit.calculate_position_size(symbol, TRADE_AMOUNT_USDT)
                                    
                                    if size > 0:
                                        # Открываем позицию
                                        order_id = await self.open_position(symbol, side, size, predicted_rate)
                                        
                                        if order_id:
                                            logger.info(
                                                f"🎯 Открыта позиция перед фандингом: {symbol} {side} "
                                                f"(фандинг через {time_until:.1f} сек, "
                                                f"ожидаемый рейт: {predicted_rate:.6f})"
                                            )
                        
                        # Проверяем, нужно ли закрыть позиции после фандинга
                        for order_id, trade_data in list(self.active_trades.items()):
                            if trade_data.get("closed", False):
                                continue
                                
                            symbol = trade_data["symbol"]
                            
                            # Ищем символ в данных фандинга
                            symbol_data = funding_df[funding_df["symbol"] == symbol]
                            
                            if not symbol_data.empty:
                                next_funding_time = symbol_data.iloc[0]["nextFundingTime"]
                                time_until = next_funding_time - current_time
                                
                                # Если прошло достаточно времени после выплаты, закрываем позицию
                                if time_until < -SECONDS_AFTER_FUNDING_TO_CLOSE:
                                    # Помечаем, что фандинг собран
                                    trade_data["funding_collected"] = True
                                    
                                    # Закрываем позицию
                                    success = await self.close_position(order_id)
                                    
                                    if success:
                                        logger.info(f"🔒 Позиция {symbol} закрыта после сбора фандинга")
                
                # Обновляем баланс периодически
                await self.update_balance()
                
                # Спим короткое время перед следующей проверкой
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Ошибка в цикле мониторинга фандинг рейтов: {e}")
                await asyncio.sleep(30)
    
    async def emergency_close_all_positions(self):
        """Экстренное закрытие всех позиций"""
        logger.warning("⚠️ Экстренное закрытие всех позиций...")
        
        for order_id in list(self.active_trades.keys()):
            try:
                await self.close_position(order_id)
            except Exception as e:
                logger.error(f"Ошибка при экстренном закрытии позиции {order_id}: {e}")
        
        await self.send_telegram_message("⚠️ Все позиции экстренно закрыты!")
    
    def setup_signal_handlers(self):
        """Настройка обработчиков сигналов для корректного завершения"""
        def signal_handler(signum, frame):
            logger.info(f"Получен сигнал {signum}, завершаем работу...")
            self.should_run = False
            
            # Запускаем экстренное закрытие позиций в новом event loop
            try:
                asyncio.create_task(self.emergency_close_all_positions())
            except:
                pass
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def start(self):
        """Запуск бота"""
        try:
            # Настраиваем обработчики сигналов
            self.setup_signal_handlers()
            
            # Инициализируем Telegram уведомления
            await self.init_telegram_notifications()
            
            # Обновляем начальный баланс
            await self.update_balance()
            logger.info(f"💰 Начальный баланс: {self.balance} USDT")
            
            # Получаем список всех пар
            symbols = await self.bybit.get_all_perpetual_symbols()
            logger.info(f"📊 Загружено {len(symbols)} торговых пар")
            
            # Получаем фандинг рейты
            funding_df = await self.get_funding_rates()
            logger.info(f"📈 Загружено {len(funding_df)} записей о фандинг рейтах")
            
            # Сохраняем статус бота в файл
            self.save_status()
            
            # Запускаем мониторинг фандинг рейтов
            await self.monitor_funding_rates()
            
        except asyncio.CancelledError:
            logger.info("Получен сигнал остановки")
            self.should_run = False
            await self.emergency_close_all_positions()
            raise
            
        except Exception as e:
            logger.error(f"Ошибка при запуске бота: {e}")
            await self.send_telegram_message(f"❌ Ошибка при запуске бота: {e}")
            self.should_run = False

async def main():
    """Главная функция запуска бота"""
    try:
        # Проверяем, что все необходимые переменные окружения установлены
        if not BYBIT_API_KEY or not BYBIT_API_SECRET:
            logger.error("Не установлены API ключи Bybit. Проверьте .env файл")
            return 1
        
        logger.info("🚀 Запуск фандинг арбитраж бота...")
        logger.info(f"💵 Сумма торговли: {TRADE_AMOUNT_USDT} USDT")
        logger.info(f"📊 Минимальный фандинг рейт: {MIN_FUNDING_RATE*100:.4f}%")
        logger.info(f"⏰ Секунд до фандинга для входа: {SECONDS_BEFORE_FUNDING}")
        logger.info(f"🔝 Топ пар для торговли: {TOP_PAIRS_COUNT}")
        
        # Инициализируем и запускаем бота
        bot = TradingBot()
        await bot.start()
        
        return 0
        
    except Exception as e:
        logger.error(f"Критическая ошибка в главной функции: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))