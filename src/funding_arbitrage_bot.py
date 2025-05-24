import os
import asyncio
import datetime
import json
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

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
        
        logger.info("Инициализация торгового бота завершена")
    
    async def update_balance(self):
        """Обновление текущего баланса USDT"""
        self.balance = await self.bybit.get_wallet_balance("USDT")
        return self.balance
    
    async def get_funding_rates(self) -> pd.DataFrame:
        """Получение и обновление фандинг рейтов"""
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
            self.save_funding_rates(funding_df)
            
        return funding_df
    
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
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            # Сохраняем в файл
            with open("bot_status.json", "w") as f:
                json.dump(status_data, f, indent=4, default=self.json_serial)
            
            logger.info("Сохранен статус торгового бота")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении статуса бота: {e}")
    
    def save_funding_rates(self, funding_df: pd.DataFrame):
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
                
                # Создаем пустой файл с метаданными чтобы не было ошибок при чтении
                funding_data = {
                    "top_rates": [],
                    "update_time": datetime.datetime.now().isoformat(),
                    "total_pairs": len(funding_df),
                    "filtered_pairs": 0,
                    "min_funding_rate": MIN_FUNDING_RATE,
                    "min_funding_rate_percent": MIN_FUNDING_RATE * 100
                }
                
                with open("funding_rates.json", "w") as f:
                    json.dump(funding_data, f, indent=4, default=self.json_serial)
                
                logger.info("Сохранен пустой файл funding_rates.json (нет подходящих пар)")
                return
            
            # Получаем текущее время для расчета времени до выплаты
            current_time = datetime.datetime.now().timestamp()
            
            # Разделяем на положительные и отрицательные
            positive_df = filtered_df[filtered_df["predictedRate"] > 0]
            negative_df = filtered_df[filtered_df["predictedRate"] < 0]
            
            # Сортируем по абсолютному значению фандинг рейта
            positive_df = positive_df.sort_values("abs_rate", ascending=False)
            negative_df = negative_df.sort_values("abs_rate", ascending=False)
            
            # Берем топ-10 положительных и топ-10 отрицательных
            top_positive = positive_df.head(10) if not positive_df.empty else pd.DataFrame()
            top_negative = negative_df.head(10) if not negative_df.empty else pd.DataFrame()
            
            # Объединяем все топовые пары
            top_pairs_df = pd.concat([top_positive, top_negative])
            
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
                hours, remainder = divmod(time_until, 3600)
                minutes, seconds = divmod(remainder, 60)
                time_until_str = f"{int(hours)}ч {int(minutes)}м {int(seconds)}с"
                
                # Определяем направление фандинг рейта
                direction = "positive" if predicted_rate > 0 else "negative"
                
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
                    "direction": direction
                })
            
            # Сортируем все топ рейты по абсолютному значению
            top_rates.sort(key=lambda x: x["abs_rate"], reverse=True)
            
            # Сохраняем топ рейты в файл
            funding_data = {
                "top_rates": top_rates,
                "update_time": datetime.datetime.now().isoformat(),
                "total_pairs": len(funding_df),
                "filtered_pairs": len(filtered_df),
                "top_positive_count": len(top_positive),
                "top_negative_count": len(top_negative),
                "min_funding_rate": MIN_FUNDING_RATE,
                "min_funding_rate_percent": MIN_FUNDING_RATE * 100
            }
            
            with open("funding_rates.json", "w") as f:
                json.dump(funding_data, f, indent=4, default=self.json_serial)
            
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
    
    async def open_position(self, symbol: str, side: str, size: float) -> bool:
        """Открытие позиции"""
        try:
            # Получаем текущую цену
            ticker = await self.bybit.get_ticker(symbol)
            if not ticker:
                return False
                
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
                    "funding_collected": False
                }
                
                logger.info(f"Открыта позиция: {symbol} {side} {size} по цене {price}")
                
                # Обновляем статус
                self.save_status()
                
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"Исключение при открытии позиции: {e}")
            return False
    
    async def close_position(self, symbol: str, side: str, size: float, order_id: str) -> bool:
        """Закрытие позиции"""
        try:
            # Противоположная сторона для закрытия
            close_side = "Sell" if side == "Buy" else "Buy"
            
            # Получаем текущую цену
            ticker = await self.bybit.get_ticker(symbol)
            if not ticker:
                return False
                
            price = float(ticker["lastPrice"])
            
            # Закрываем позицию рыночным ордером
            order = await self.bybit.place_market_order(symbol, close_side, size, reduce_only=True)
            
            if order:
                # Рассчитываем прибыль/убыток
                if order_id in self.active_trades:
                    trade_data = self.active_trades[order_id]
                    
                    entry_price = trade_data["entry_price"]
                    pnl = 0
                    
                    if side == "Buy":  # Был лонг, теперь продаем
                        pnl = (price - entry_price) * size
                    else:  # Был шорт, теперь покупаем
                        pnl = (entry_price - price) * size
                    
                    # Добавляем приблизительную прибыль от фандинга
                    predicted_rate = self.funding_schedule[symbol]["predictedRate"]
                    funding_pnl = 0
                    
                    if (side == "Buy" and predicted_rate < 0) or (side == "Sell" and predicted_rate > 0):
                        # Мы платим фандинг
                        funding_pnl = -abs(predicted_rate) * price * size
                    else:
                        # Мы получаем фандинг
                        funding_pnl = abs(predicted_rate) * price * size
                    
                    total_pnl = pnl + funding_pnl
                    
                    # Обновляем баланс
                    await self.update_balance()
                    
                    # Удаляем из активных сделок
                    del self.active_trades[order_id]
                    
                    # Обновляем статус
                    self.save_status()
                
                logger.info(f"Закрыта позиция: {symbol} {close_side} {size} по цене {price}")
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"Исключение при закрытии позиции: {e}")
            return False
    
    async def monitor_funding_rates(self):
        """Мониторинг и обработка ближайших фандинг выплат"""
        while self.should_run:
            try:
                # Обновляем информацию о фандинг рейтах
                funding_df = await self.get_funding_rates()
                
                if not funding_df.empty:
                    # Добавляем колонку с абсолютным значением фандинг рейта
                    funding_df["abs_rate"] = funding_df["predictedRate"].abs()
                    
                    # Фильтруем только те пары, у которых фандинг рейт больше минимального
                    filtered_df = funding_df[funding_df["abs_rate"] >= MIN_FUNDING_RATE]
                    
                    if not filtered_df.empty:
                        # Находим ближайшую выплату
                        current_time = datetime.datetime.now().timestamp()
                        
                        # Разделяем на положительные и отрицательные
                        positive_df = filtered_df[filtered_df["predictedRate"] > 0]
                        negative_df = filtered_df[filtered_df["predictedRate"] < 0]
                        
                        # Сортируем по абсолютному значению фандинг рейта
                        positive_df = positive_df.sort_values("abs_rate", ascending=False)
                        negative_df = negative_df.sort_values("abs_rate", ascending=False)
                        
                        # Берем топ-10 положительных и топ-10 отрицательных
                        top_positive = positive_df.head(10) if not positive_df.empty else pd.DataFrame()
                        top_negative = negative_df.head(10) if not negative_df.empty else pd.DataFrame()
                        
                        # Объединяем все топовые пары для мониторинга и торговли
                        top_pairs_df = pd.concat([top_positive, top_negative])
                        
                        # Проходим по всем топовым парам
                        for _, row in top_pairs_df.iterrows():
                            symbol = row["symbol"]
                            next_funding_time = row["nextFundingTime"]
                            predicted_rate = row["predictedRate"]
                            abs_rate = row["abs_rate"]
                            time_until = next_funding_time - current_time
                            
                            # Проверяем, подходит ли время для открытия позиции
                            if SECONDS_BEFORE_FUNDING >= time_until > 0:
                                # Определяем сторону для позиции
                                side = "Sell" if predicted_rate > 0 else "Buy"
                                
                                # Проверяем, что у нас нет открытой позиции по этому символу
                                symbol_active = False
                                for trade_data in self.active_trades.values():
                                    if trade_data["symbol"] == symbol:
                                        symbol_active = True
                                        break
                                        
                                if not symbol_active:
                                    # Рассчитываем размер позиции
                                    size = await self.bybit.calculate_position_size(symbol, TRADE_AMOUNT_USDT)
                                    
                                    if size > 0:
                                        # Открываем позицию
                                        success = await self.open_position(symbol, side, size)
                                        
                                        if success:
                                            logger.info(
                                                f"Открыта позиция перед фандингом: {symbol} {side} "
                                                f"(фандинг через {time_until:.1f} сек, "
                                                f"ожидаемый рейт: {predicted_rate:.6f}, "
                                                f"абсолютное значение: {abs_rate:.6f})"
                                            )
                        
                        # Проверяем, нужно ли закрыть позиции после фандинга
                        for order_id, trade_data in list(self.active_trades.items()):
                            symbol = trade_data["symbol"]
                            
                            # Ищем символ в текущих данных
                            symbol_row = filtered_df[filtered_df["symbol"] == symbol]
                            
                            if not symbol_row.empty:
                                next_funding_time = symbol_row.iloc[0]["nextFundingTime"]
                                time_until = next_funding_time - current_time
                                
                                # Если прошло более 5 секунд после выплаты и фандинг еще не собран
                                if time_until < -5 and not trade_data["funding_collected"]:
                                    # Помечаем, что фандинг собран
                                    trade_data["funding_collected"] = True
                                    
                                    # Закрываем позицию
                                    await self.close_position(
                                        trade_data["symbol"], 
                                        trade_data["side"], 
                                        trade_data["size"], 
                                        order_id
                                    )
                
                # Спим короткое время
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Ошибка в цикле мониторинга фандинг рейтов: {e}")
                await asyncio.sleep(30)
    
    async def start(self):
        """Запуск бота"""
        try:
            # Обновляем начальный баланс
            await self.update_balance()
            
            # Получаем список всех пар
            symbols = await self.bybit.get_all_perpetual_symbols()
            logger.info(f"Загружено {len(symbols)} торговых пар")
            
            # Получаем фандинг рейты
            funding_df = await self.get_funding_rates()
            logger.info(f"Загружено {len(funding_df)} записей о фандинг рейтах")
            
            # Сохраняем статус бота в файл
            self.save_status()
            
            # Запускаем мониторинг фандинг рейтов
            await self.monitor_funding_rates()
            
        except asyncio.CancelledError:
            logger.info("Получен сигнал остановки")
            self.should_run = False
            raise
            
        except Exception as e:
            logger.error(f"Ошибка при запуске бота: {e}")
            self.should_run = False

async def main():
    """Главная функция запуска бота"""
    try:
        # Проверяем, что все необходимые переменные окружения установлены
        if not BYBIT_API_KEY or not BYBIT_API_SECRET:
            logger.error("Не установлены API ключи Bybit. Проверьте .env файл")
            return 1
        
        # Инициализируем и запускаем бота
        bot = TradingBot()
        await bot.start()
        
        return 0
        
    except Exception as e:
        logger.error(f"Критическая ошибка в главной функции: {e}")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(asyncio.run(main()))