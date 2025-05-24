import asyncio
import datetime
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import pandas as pd
from pybit.unified_trading import HTTP, WebSocket

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
        
        # Инициализация WebSocket клиента
        self.ws_private = WebSocket(
            testnet=testnet,
            channel_type="private",
            api_key=api_key,
            api_secret=api_secret,
        )
        
        # Кэш информации о символах
        self.symbols_info = {}
        
    async def get_wallet_balance(self, coin: str = "USDT") -> float:
        """
        Получение баланса кошелька
        
        Args:
            coin: Валюта (по умолчанию USDT)
            
        Returns:
            float: Баланс в указанной валюте
        """
        try:
            wallet_balance = self.session.get_wallet_balance(accountType="UNIFIED", coin=coin)
            if wallet_balance["retCode"] == 0:
                balance = float(wallet_balance["result"]["list"][0]["coin"][0]["walletBalance"])
                logger.info(f"Текущий баланс {coin}: {balance}")
                return balance
            else:
                logger.error(f"Ошибка при получении баланса: {wallet_balance}")
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
            instruments = self.session.get_instruments_info(category="linear")
            if instruments["retCode"] == 0:
                symbols = []
                
                for symbol in instruments["result"]["list"]:
                    if symbol["quoteCoin"] == "USDT" and symbol["status"] == "Trading":
                        symbols.append(symbol["symbol"])
                        
                        # Сохраняем информацию о символе в кэш
                        self.symbols_info[symbol["symbol"]] = {
                            "lotSizeFilter": symbol["lotSizeFilter"],
                            "priceFilter": symbol["priceFilter"],
                            "baseCoin": symbol["baseCoin"],
                            "quoteCoin": symbol["quoteCoin"]
                        }
                
                logger.info(f"Получено {len(symbols)} символов торговых пар")
                return symbols
            else:
                logger.error(f"Ошибка при получении символов: {instruments}")
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
            symbols = await self.get_all_perpetual_symbols()
            
            all_funding_data = []
            
            for symbol in symbols:
                funding_info = self.session.get_funding_rate_history(
                    category="linear",
                    symbol=symbol,
                    limit=1
                )
                
                if funding_info["retCode"] == 0 and funding_info["result"]["list"]:
                    data = funding_info["result"]["list"][0]
                    
                    # Получаем информацию о следующей выплате
                    mark_price_info = self.session.get_tickers(
                        category="linear",
                        symbol=symbol
                    )
                    
                    next_funding_time = None
                    predicted_rate = None
                    
                    if mark_price_info["retCode"] == 0 and mark_price_info["result"]["list"]:
                        ticker_data = mark_price_info["result"]["list"][0]
                        next_funding_time = int(ticker_data["nextFundingTime"]) / 1000  # в секундах
                        predicted_rate = float(ticker_data["fundingRate"])
                    
                    if next_funding_time:
                        all_funding_data.append({
                            "symbol": symbol,
                            "fundingRate": float(data["fundingRate"]),
                            "predictedRate": predicted_rate,
                            "nextFundingTime": next_funding_time,
                            "timestamp": datetime.datetime.fromtimestamp(next_funding_time)
                        })
            
            df = pd.DataFrame(all_funding_data)
            if not df.empty:
                # Сортируем по времени следующей выплаты
                df = df.sort_values("nextFundingTime")
                
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
            ticker = self.session.get_tickers(category="linear", symbol=symbol)
            if ticker["retCode"] == 0 and ticker["result"]["list"]:
                return ticker["result"]["list"][0]
            else:
                logger.error(f"Ошибка при получении тикера для {symbol}: {ticker}")
                return None
        except Exception as e:
            logger.error(f"Исключение при получении тикера: {e}")
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
                return 0
                
            price = float(ticker["lastPrice"])
            
            # Определяем количество монет, которое можно купить на amount_usdt
            size = amount_usdt / price
            
            # Получаем информацию о минимальном размере ордера из кэша
            if symbol in self.symbols_info:
                lot_size_filter = self.symbols_info[symbol]["lotSizeFilter"]
            else:
                # Если нет в кэше, запрашиваем заново
                instrument_info = self.session.get_instruments_info(category="linear", symbol=symbol)
                if instrument_info["retCode"] == 0 and instrument_info["result"]["list"]:
                    lot_size_filter = instrument_info["result"]["list"][0]["lotSizeFilter"]
                    
                    # Обновляем кэш
                    if symbol not in self.symbols_info:
                        self.symbols_info[symbol] = {}
                    self.symbols_info[symbol]["lotSizeFilter"] = lot_size_filter
                else:
                    logger.error(f"Не удалось получить информацию о {symbol}")
                    return 0
            
            min_order_qty = float(lot_size_filter["minOrderQty"])
            qty_step = float(lot_size_filter["qtyStep"])
            
            # Округляем размер до ближайшего шага
            size = max(min_order_qty, round(size / qty_step) * qty_step)
            
            logger.info(f"Рассчитанный размер позиции для {symbol}: {size}")
            return size
            
        except Exception as e:
            logger.error(f"Ошибка при расчете размера позиции: {e}")
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
                
            order = self.session.place_order(**order_params)
            
            if order["retCode"] == 0:
                logger.info(f"Размещен ордер: {symbol} {side} {size}")
                return order["result"]
            else:
                logger.error(f"Ошибка при размещении ордера: {order}")
                return None
                
        except Exception as e:
            logger.error(f"Исключение при размещении ордера: {e}")
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
            params = {"category": "linear"}
            if symbol:
                params["symbol"] = symbol
                
            positions = self.session.get_positions(**params)
            
            if positions["retCode"] == 0:
                return positions["result"]["list"]
            else:
                logger.error(f"Ошибка при получении позиций: {positions}")
                return []
                
        except Exception as e:
            logger.error(f"Исключение при получении позиций: {e}")
            return []