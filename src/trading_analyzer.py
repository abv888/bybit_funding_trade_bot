import logging
import datetime
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

class TradingAnalyzer:
    """Класс для анализа и оптимизации торговых стратегий"""
    
    @staticmethod
    def analyze_funding_history(funding_history: pd.DataFrame) -> Dict:
        """
        Анализ истории фандинг рейтов для поиска оптимальных торговых параметров
        
        Args:
            funding_history: Датафрейм с историей фандинг рейтов
            
        Returns:
            Dict: Результаты анализа с рекомендуемыми параметрами
        """
        if funding_history.empty:
            logger.warning("Пустой датафрейм с историей фандинг рейтов")
            return {}
        
        try:
            # Основные статистические данные
            stats = {
                "mean_rate": funding_history["fundingRate"].mean(),
                "median_rate": funding_history["fundingRate"].median(),
                "max_rate": funding_history["fundingRate"].max(),
                "min_rate": funding_history["fundingRate"].min(),
                "std_rate": funding_history["fundingRate"].std(),
                "positive_rate_count": (funding_history["fundingRate"] > 0).sum(),
                "negative_rate_count": (funding_history["fundingRate"] < 0).sum(),
                "zero_rate_count": (funding_history["fundingRate"] == 0).sum()
            }
            
            # Анализ по абсолютному значению рейта
            funding_history["abs_rate"] = funding_history["fundingRate"].abs()
            
            # Определение оптимального порога для торговли
            percentiles = [50, 75, 90, 95, 99]
            thresholds = {}
            
            for p in percentiles:
                threshold = funding_history["abs_rate"].quantile(p/100)
                thresholds[f"percentile_{p}"] = threshold
            
            # Оценка потенциальной прибыли для разных порогов
            profit_estimates = {}
            
            for p, threshold in thresholds.items():
                # Считаем, что мы торгуем только рейты выше порога
                filtered_rates = funding_history[funding_history["abs_rate"] >= threshold]
                
                if not filtered_rates.empty:
                    # Предполагаем, что для каждой торговой пары используем 10 USDT
                    # и что средняя цена актива = 1 USDT для упрощения
                    total_profit = filtered_rates["abs_rate"].sum() * 10
                    avg_profit_per_trade = filtered_rates["abs_rate"].mean() * 10
                    trade_count = len(filtered_rates)
                    
                    profit_estimates[p] = {
                        "threshold": threshold,
                        "trade_count": trade_count,
                        "total_profit": total_profit,
                        "avg_profit_per_trade": avg_profit_per_trade
                    }
            
            # Лучшие пары для торговли (с наибольшим средним абсолютным фандинг рейтом)
            top_pairs = (
                funding_history.groupby("symbol")["abs_rate"]
                .mean()
                .sort_values(ascending=False)
                .head(10)
                .to_dict()
            )
            
            # Рекомендуемые параметры
            recommended = {
                "min_funding_rate": thresholds.get("percentile_75", 0.0001),
                "top_symbols": list(top_pairs.keys()),
                "trade_seconds_before_funding": 10,  # Рекомендуемое значение
                "expected_profit_per_day": profit_estimates.get("75", {}).get("total_profit", 0) / len(funding_history) * 3  # Примерная оценка (3 выплаты в день)
            }
            
            return {
                "stats": stats,
                "thresholds": thresholds,
                "profit_estimates": profit_estimates,
                "top_pairs": top_pairs,
                "recommended": recommended
            }
            
        except Exception as e:
            logger.error(f"Ошибка при анализе истории фандинг рейтов: {e}")
            return {}
    
    @staticmethod
    def simulate_trading_strategy(
        funding_history: pd.DataFrame,
        min_funding_rate: float = 0.0001,
        trade_amount: float = 10.0,
        seconds_before_funding: int = 10,
        price_volatility: float = 0.001  # Примерная волатильность цены в % за время удержания позиции
    ) -> Dict:
        """
        Симуляция торговой стратегии на исторических данных
        
        Args:
            funding_history: Датафрейм с историей фандинг рейтов
            min_funding_rate: Минимальный фандинг рейт для совершения сделки
            trade_amount: Сумма в USDT для каждой сделки
            seconds_before_funding: За сколько секунд до выплаты открывать позицию
            price_volatility: Ожидаемая волатильность цены во время удержания позиции
            
        Returns:
            Dict: Результаты симуляции
        """
        if funding_history.empty:
            logger.warning("Пустой датафрейм с историей фандинг рейтов")
            return {}
        
        try:
            # Фильтруем сделки по минимальному рейту
            funding_history["abs_rate"] = funding_history["fundingRate"].abs()
            valid_trades = funding_history[funding_history["abs_rate"] >= min_funding_rate].copy()
            
            if valid_trades.empty:
                logger.warning(f"Нет сделок с фандинг рейтом >= {min_funding_rate}")
                return {
                    "profit": 0,
                    "trade_count": 0,
                    "avg_profit_per_trade": 0,
                    "success_rate": 0,
                    "roi": 0
                }
            
            # Симулируем каждую сделку
            trades = []
            
            for _, row in valid_trades.iterrows():
                symbol = row["symbol"]
                funding_rate = row["fundingRate"]
                abs_rate = row["abs_rate"]
                
                # Определяем сторону для позиции
                side = "Sell" if funding_rate > 0 else "Buy"
                
                # Ожидаемая прибыль от фандинга
                funding_profit = abs_rate * trade_amount
                
                # Симуляция случайного изменения цены за время удержания позиции
                # Используем нормальное распределение для изменения цены
                price_change_pct = np.random.normal(0, price_volatility)
                price_impact = trade_amount * price_change_pct
                
                # Если мы в лонге, то положительное изменение цены = прибыль
                # Если мы в шорте, то отрицательное изменение цены = прибыль
                price_profit = price_impact if side == "Buy" else -price_impact
                
                # Общая прибыль/убыток
                total_profit = funding_profit + price_profit
                
                trades.append({
                    "symbol": symbol,
                    "side": side,
                    "funding_rate": funding_rate,
                    "funding_profit": funding_profit,
                    "price_change_pct": price_change_pct,
                    "price_profit": price_profit,
                    "total_profit": total_profit,
                    "is_profitable": total_profit > 0
                })
            
            # Создаем датафрейм с результатами
            trades_df = pd.DataFrame(trades)
            
            # Рассчитываем статистику
            total_profit = trades_df["total_profit"].sum()
            trade_count = len(trades_df)
            avg_profit_per_trade = total_profit / trade_count if trade_count > 0 else 0
            success_rate = (trades_df["is_profitable"].sum() / trade_count) * 100 if trade_count > 0 else 0
            total_investment = trade_count * trade_amount
            roi = (total_profit / total_investment) * 100 if total_investment > 0 else 0
            
            # Статистика по символам
            symbol_stats = trades_df.groupby("symbol").agg({
                "total_profit": "sum",
                "is_profitable": "mean",
                "price_profit": "sum",
                "funding_profit": "sum"
            }).sort_values("total_profit", ascending=False)
            
            # Преобразуем is_profitable в процентный успех
            symbol_stats["success_rate"] = symbol_stats["is_profitable"] * 100
            
            return {
                "profit": total_profit,
                "trade_count": trade_count,
                "avg_profit_per_trade": avg_profit_per_trade,
                "success_rate": success_rate,
                "roi": roi,
                "symbol_stats": symbol_stats.to_dict(),
                "trades": trades
            }
            
        except Exception as e:
            logger.error(f"Ошибка при симуляции торговой стратегии: {e}")
            return {}
    
    @staticmethod
    def optimize_parameters(
        funding_history: pd.DataFrame,
        min_rates: List[float] = [0.00005, 0.0001, 0.00025, 0.0005, 0.001],
        volatility_ranges: List[float] = [0.0005, 0.001, 0.002, 0.005]
    ) -> Dict:
        """
        Оптимизация параметров стратегии
        
        Args:
            funding_history: Датафрейм с историей фандинг рейтов
            min_rates: Список значений минимального фандинг рейта для тестирования
            volatility_ranges: Список значений волатильности для тестирования
            
        Returns:
            Dict: Оптимальные параметры
        """
        if funding_history.empty:
            logger.warning("Пустой датафрейм с историей фандинг рейтов")
            return {}
        
        try:
            results = []
            
            for min_rate in min_rates:
                for volatility in volatility_ranges:
                    sim_result = TradingAnalyzer.simulate_trading_strategy(
                        funding_history,
                        min_funding_rate=min_rate,
                        price_volatility=volatility
                    )
                    
                    results.append({
                        "min_funding_rate": min_rate,
                        "price_volatility": volatility,
                        "profit": sim_result.get("profit", 0),
                        "trade_count": sim_result.get("trade_count", 0),
                        "success_rate": sim_result.get("success_rate", 0),
                        "roi": sim_result.get("roi", 0)
                    })
            
            # Создаем датафрейм с результатами
            results_df = pd.DataFrame(results)
            
            if not results_df.empty:
                # Находим оптимальные параметры по ROI
                best_by_roi = results_df.loc[results_df["roi"].idxmax()]
                
                # Находим оптимальные параметры по абсолютной прибыли
                best_by_profit = results_df.loc[results_df["profit"].idxmax()]
                
                # Находим оптимальные параметры по успешности сделок
                best_by_success = results_df.loc[results_df["success_rate"].idxmax()]
                
                return {
                    "best_by_roi": best_by_roi.to_dict(),
                    "best_by_profit": best_by_profit.to_dict(),
                    "best_by_success": best_by_success.to_dict(),
                    "all_results": results_df.to_dict()
                }
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Ошибка при оптимизации параметров: {e}")
            return {}