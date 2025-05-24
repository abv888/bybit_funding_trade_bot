import asyncio
import datetime
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
import time

import pandas as pd
from pybit.unified_trading import HTTP

logger = logging.getLogger(__name__)

class BybitClient:
    """Класс для работы с API Bybit"""
    
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        """
        Инициализация клиента Bybit
        
        Args:
            api_key: API ключ
            api_secret: API секрет
            testnet: Использовать тестовую сеть
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        
        # Инициализация HTTP клиента
        self.session = HTTP(
            testnet=testnet,
            api_key=api_key,
            api_secret=api_secret,
        )
        
        # Кэш информации о символах
        self.symbols_info = {}
        
        # Лимиты API запросов
        self.last_request_time = 0
        self.min_request_interval = 0.1  # Минимальный интервал между запросами (100ms)
        
        logger.info(f"Инициализирован Bybit клиент (testnet: {testnet})")
    
    async def _rate_limit_check(self):
        """Проверка лимитов API запросов"""
        current_time = time.time()
        time_diff = current_time - self.last_request_time
        
        if time_diff < self.min_request_interval:
            sleep_time = self.min_request_interval - time_diff
            await asyncio.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    async def get_wallet_balance(self, coin: str = "USDT") -> float:
        """
        Получение баланса кошелька
        
        Args:
            coin: Валюта (по умолчанию USDT)
            
        Returns:
            float: Баланс в указанной валюте
        """
        try:
            await self._rate_limit_check()
            
            wallet_balance = self.session.get_wallet_balance(accountType="UNIFIED", coin=coin)
            
            if wallet_balance["retCode"] == 0 and wallet_balance["result"]["list"]:
                coins = wallet_balance["result"]["list"][0]["coin"]
                
                # Ищем нужную монету
                for coin_info in coins:
                    if coin_info["coin"] == coin:
                        balance = float(coin_info["walletBalance"])
                        logger.info(f"Текущий баланс {coin}: {balance}")
                        return balance
                
                # Если монета не найдена, возвращаем 0
                logger.warning(f"Монета {coin} не найдена в кошельке")
                return 0.0
            else:
                logger.error(f"Ошибка при получении баланса: {wallet_balance.get('retMsg', 'Unknown error')}")
                return 0.0
                
        except Exception as e:
            logger.error(f"Исключение при получении баланса: {e}")
            return 0.0
    
    async def get_all_perpetual_symbols(self) -> List[str]:
        """
        Получение всех бессрочных контрактов с USDT
        
        Returns:
            List[str]: Список символов
        """
        try:
            await self._rate_limit_check()
            
            instruments = self.session.get_instruments_info(category="linear")
            
            if instruments["retCode"] == 0:
                symbols = []
                
                for symbol in instruments["result"]["list"]:
                    # Фильтруем только активные USDT пары
                    if (symbol["quoteCoin"] == "USDT" and 
                        symbol["status"] == "Trading" and
                        symbol["contractType"] == "LinearPerpetual"):
                        
                        symbols.append(symbol["symbol"])
                        
                        # Сохраняем информацию о символе в кэш
                        self.symbols_info[symbol["symbol"]] = {
                            "lotSizeFilter": symbol["lotSizeFilter"],
                            "priceFilter": symbol["priceFilter"],
                            "baseCoin": symbol["baseCoin"],
                            "quoteCoin": symbol["quoteCoin"],
                            "minOrderQty": float(symbol["lotSizeFilter"]["minOrderQty"]),
                            "qtyStep": float(symbol["lotSizeFilter"]["qtyStep"]),
                            "tickSize": float(symbol["priceFilter"]["tickSize"])
                        }
                
                logger.info(f"Получено {len(symbols)} символов торговых пар")
                return symbols
            else:
                logger.error(f"Ошибка при получении символов: {instruments.get('retMsg', 'Unknown error')}")
                return []
                
        except Exception as e:
            logger.error(f"Исключение при получении символов: {e}")
            return []
    
    async def get_funding_rates(self) -> pd.DataFrame:
        """
        Получение текущих фандинг рейтов для всех пар
        
        Returns:
            pd.DataFrame: Датафрейм с информацией о фандинг рейтах
        """
        try:
            # Получаем все символы, если кэш пуст
            if not self.symbols_info:
                await self.get_all_perpetual_symbols()
            
            symbols = list(self.symbols_info.keys())
            all_funding_data = []
            
            # Обрабатываем символы батчами для избежания лимитов API
            batch_size = 50
            for i in range(0, len(symbols), batch_size):
                batch_symbols = symbols[i:i + batch_size]
                
                for symbol in batch_symbols:
                    try:
                        await self._rate_limit_check()
                        
                        # Получаем тикер с информацией о фандинге
                        ticker_info = self.session.get_tickers(
                            category="linear",
                            symbol=symbol
                        )
                        
                        if ticker_info["retCode"] == 0 and ticker_info["result"]["list"]:
                            ticker_data = ticker_info["result"]["list"][0]
                            
                            next_funding_time = int(ticker_data["nextFundingTime"])
                            predicted_rate = float(ticker_data["fundingRate"]) if ticker_data["fundingRate"] else 0.0
                            
                            # Проверяем, что данные валидны
                            if next_funding_time > 0 and predicted_rate != 0:
                                all_funding_data.append({
                                    "symbol": symbol,
                                    "fundingRate": predicted_rate,  # Текущий рейт
                                    "predictedRate": predicted_rate,  # Предсказанный рейт
                                    "nextFundingTime": next_funding_time / 1000,  # в секундах
                                    "timestamp": datetime.datetime.fromtimestamp(next_funding_time / 1000),
                                    "lastPrice": float(ticker_data.get("lastPrice", 0))
                                })
                        
                    except Exception as e:
                        logger.warning(f"Ошибка при получении фандинг рейта для {symbol}: {e}")
                        continue
                
                # Небольшая пауза между батчами
                if i + batch_size < len(symbols):
                    await asyncio.sleep(0.5)
            
            df = pd.DataFrame(all_funding_data)
            
            if not df.empty:
                # Сортируем по времени следующей выплаты
                df = df.sort_values("nextFundingTime")
                logger.info(f"Получено {len(df)} записей о фандинг рейтах")
            else:
                logger.warning("Не получено данных о фандинг рейтах")
                
            return df
        
        except Exception as e:
            logger.error(f"Ошибка при получении фандинг рейтов: {e}")
            return pd.DataFrame()
    
    async def get_ticker(self, symbol: str) -> Optional[Dict]:
        """
        Получение текущих данных тикера
        
        Args:
            symbol: Символ торговой пары
            
        Returns:
            Optional[Dict]: Данные тикера или None в случае ошибки
        """
        try:
            await self._rate_limit_check()
            
            ticker = self.session.get_tickers(category="linear", symbol=symbol)
            
            if ticker["retCode"] == 0 and ticker["result"]["list"]:
                return ticker["result"]["list"][0]
            else:
                logger.error(f"Ошибка при получении тикера для {symbol}: {ticker.get('retMsg', 'Unknown error')}")
                return None
                
        except Exception as e:
            logger.error(f"Исключение при получении тикера для {symbol}: {e}")
            return None
    
    async def calculate_position_size(self, symbol: str, amount_usdt: float) -> float:
        """
        Расчет размера позиции в зависимости от текущей цены
        
        Args:
            symbol: Символ торговой пары
            amount_usdt: Сумма в USDT для позиции
            
        Returns:
            float: Размер позиции в единицах базовой валюты
        """
        try:
            ticker = await self.get_ticker(symbol)
            if not ticker:
                logger.error(f"Не удалось получить тикер для {symbol}")
                return 0
                
            price = float(ticker["lastPrice"])
            
            # Определяем количество монет, которое можно купить на amount_usdt
            size = amount_usdt / price
            
            # Получаем информацию о минимальном размере ордера из кэша
            symbol_info = self.symbols_info.get(symbol)
            
            if not symbol_info:
                # Если нет в кэше, запрашиваем заново
                await self.get_all_perpetual_symbols()
                symbol_info = self.symbols_info.get(symbol)
                
                if not symbol_info:
                    logger.error(f"Не удалось получить информацию о {symbol}")
                    return 0
            
            min_order_qty = symbol_info["minOrderQty"]
            qty_step = symbol_info["qtyStep"]
            
            # Округляем размер до ближайшего шага
            if size < min_order_qty:
                logger.warning(f"Размер позиции {size} меньше минимального {min_order_qty} для {symbol}")
                return 0
            
            # Округляем вниз до ближайшего шага
            size = int(size / qty_step) * qty_step
            size = max(min_order_qty, size)
            
            logger.info(f"Рассчитанный размер позиции для {symbol}: {size} (цена: {price})")
            return size
            
        except Exception as e:
            logger.error(f"Ошибка при расчете размера позиции для {symbol}: {e}")
            return 0
    
    async def place_market_order(self, symbol: str, side: str, size: float, reduce_only: bool = False) -> Optional[Dict]:
        """
        Размещение рыночного ордера
        
        Args:
            symbol: Символ торговой пары
            side: Сторона ордера ("Buy" или "Sell")
            size: Размер ордера
            reduce_only: Только для закрытия позиции
            
        Returns:
            Optional[Dict]: Результат размещения ордера или None в случае ошибки
        """
        try:
            await self._rate_limit_check()
            
            order_params = {
                "category": "linear",
                "symbol": symbol,
                "side": side,
                "orderType": "Market",
                "qty": str(size),
                "positionIdx": 0,  # 0 для режима one-way
                "timeInForce": "IOC",  # Immediate Or Cancel
            }
            
            if reduce_only:
                order_params["reduceOnly"] = True
            
            logger.info(f"Размещение ордера: {symbol} {side} {size} (reduce_only: {reduce_only})")
            
            order = self.session.place_order(**order_params)
            
            if order["retCode"] == 0:
                logger.info(f"✅ Ордер размещен успешно: {symbol} {side} {size}")
                return order["result"]
            else:
                error_msg = order.get("retMsg", "Unknown error")
                logger.error(f"❌ Ошибка при размещении ордера: {error_msg}")
                return None
                
        except Exception as e:
            logger.error(f"Исключение при размещении ордера {symbol} {side} {size}: {e}")
            return None
    
    async def get_positions(self, symbol: Optional[str] = None) -> List[Dict]:
        """
        Получение открытых позиций
        
        Args:
            symbol: Символ торговой пары (опционально)
            
        Returns:
            List[Dict]: Список открытых позиций
        """
        try:
            await self._rate_limit_check()
            
            params = {"category": "linear"}
            if symbol:
                params["symbol"] = symbol
                
            positions = self.session.get_positions(**params)
            
            if positions["retCode"] == 0:
                # Фильтруем только открытые позиции
                open_positions = []
                for pos in positions["result"]["list"]:
                    if float(pos["size"]) > 0:  # Позиция открыта
                        open_positions.append(pos)
                
                logger.info(f"Получено {len(open_positions)} открытых позиций")
                return open_positions
            else:
                logger.error(f"Ошибка при получении позиций: {positions.get('retMsg', 'Unknown error')}")
                return []
                
        except Exception as e:
            logger.error(f"Исключение при получении позиций: {e}")
            return []
    
    async def check_api_connection(self) -> bool:
        """
        Проверка подключения к API
        
        Returns:
            bool: True если подключение работает
        """
        try:
            await self._rate_limit_check()
            
            # Простой запрос для проверки подключения
            server_time = self.session.get_server_time()
            
            if server_time["retCode"] == 0:
                logger.info("✅ Подключение к Bybit API работает")
                return True
            else:
                logger.error(f"❌ Ошибка подключения к API: {server_time.get('retMsg', 'Unknown error')}")
                return False
                
        except Exception as e:
            logger.error(f"Исключение при проверке подключения к API: {e}")
            return False
    
    async def get_account_info(self) -> Optional[Dict]:
        """
        Получение информации об аккаунте
        
        Returns:
            Optional[Dict]: Информация об аккаунте или None
        """
        try:
            await self._rate_limit_check()
            
            account_info = self.session.get_wallet_balance(accountType="UNIFIED")
            
            if account_info["retCode"] == 0:
                return account_info["result"]
            else:
                logger.error(f"Ошибка при получении информации об аккаунте: {account_info.get('retMsg', 'Unknown error')}")
                return None
                
        except Exception as e:
            logger.error(f"Исключение при получении информации об аккаунте: {e}")
            return None