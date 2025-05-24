import asyncio
import argparse
import json
import logging
import os
from datetime import datetime
from dotenv import load_dotenv

import pandas as pd

from bybit_client import BybitClient
from funding_data_collector import FundingDataCollector
from trading_analyzer import TradingAnalyzer

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("analyzer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
MIN_FUNDING_RATE = float(os.getenv("MIN_FUNDING_RATE"))

async def collect_data(days: int = 7) -> pd.DataFrame:
    """
    Сбор исторических данных о фандинг рейтах
    
    Args:
        days: Количество дней для сбора данных
        
    Returns:
        pd.DataFrame: Датафрейм с историческими данными
    """
    bybit = BybitClient(api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET)
    collector = FundingDataCollector(bybit)
    
    # Сначала собираем текущие данные
    await collector.collect_current_funding_rates()
    
    # Затем загружаем исторические данные
    historical_data = collector.load_historical_funding_data(days=days)
    
    return historical_data

async def analyze_data(historical_data: pd.DataFrame) -> None:
    """
    Анализ исторических данных и оптимизация параметров
    
    Args:
        historical_data: Датафрейм с историческими данными
    """
    if historical_data.empty:
        logger.error("Нет данных для анализа")
        return
    
    # Анализ истории фандинг рейтов
    analysis_result = TradingAnalyzer.analyze_funding_history(historical_data)
    
    if not analysis_result:
        logger.error("Не удалось проанализировать данные")
        return
    
    # Сохраняем результаты анализа
    output_dir = "analysis_results"
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    with open(os.path.join(output_dir, f"analysis_{timestamp}.json"), "w") as f:
        json.dump(analysis_result, f, indent=4, default=str)
    
    logger.info("Анализ истории фандинг рейтов завершен")
    
    # Симуляция торговой стратегии с рекомендуемыми параметрами
    use_recommended = os.getenv("USE_RECOMMENDED_PARAMS", "false").lower() == "true"
    min_rate = analysis_result.get("recommended", {}).get("min_funding_rate", MIN_FUNDING_RATE) if use_recommended else MIN_FUNDING_RATE
    
    simulation_result = TradingAnalyzer.simulate_trading_strategy(
        historical_data,
        min_funding_rate=min_rate
    )
    
    # Сохраняем результаты симуляции
    with open(os.path.join(output_dir, f"simulation_{timestamp}.json"), "w") as f:
        json.dump(simulation_result, f, indent=4, default=str)
    
    logger.info("Симуляция торговой стратегии завершена")
    
    # Оптимизация параметров
    optimization_result = TradingAnalyzer.optimize_parameters(historical_data)
    
    # Сохраняем результаты оптимизации
    with open(os.path.join(output_dir, f"optimization_{timestamp}.json"), "w") as f:
        json.dump(optimization_result, f, indent=4, default=str)
    
    logger.info("Оптимизация параметров завершена")
    
    # Выводим рекомендуемые параметры
    print("\n=== Рекомендуемые параметры торговли ===")
    
    if "recommended" in analysis_result:
        print(f"Минимальный фандинг рейт: {analysis_result['recommended']['min_funding_rate']:.6f}")
        print(f"Топ-5 пар для торговли: {', '.join(analysis_result['recommended']['top_symbols'][:5])}")
        print(f"Ожидаемая прибыль в день: {analysis_result['recommended']['expected_profit_per_day']:.4f} USDT\n")
    
    if "best_by_roi" in optimization_result:
        print("=== Оптимальные параметры по ROI ===")
        print(f"Минимальный фандинг рейт: {optimization_result['best_by_roi']['min_funding_rate']:.6f}")
        print(f"Ожидаемая волатильность: {optimization_result['best_by_roi']['price_volatility']:.6f}")
        print(f"ROI: {optimization_result['best_by_roi']['roi']:.2f}%")
        print(f"Успешность сделок: {optimization_result['best_by_roi']['success_rate']:.2f}%\n")
    
    if "best_by_success" in optimization_result:
        print("=== Оптимальные параметры по успешности сделок ===")
        print(f"Минимальный фандинг рейт: {optimization_result['best_by_success']['min_funding_rate']:.6f}")
        print(f"Ожидаемая волатильность: {optimization_result['best_by_success']['price_volatility']:.6f}")
        print(f"Успешность сделок: {optimization_result['best_by_success']['success_rate']:.2f}%")
        print(f"ROI: {optimization_result['best_by_success']['roi']:.2f}%\n")
    
async def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description="Анализ и оптимизация стратегии арбитража фандинг рейтов")
    
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Количество дней для анализа (по умолчанию 7)"
    )
    
    parser.add_argument(
        "--collect-only",
        action="store_true",
        help="Только сбор данных без анализа"
    )
    
    args = parser.parse_args()
    
    try:
        logger.info(f"Запуск анализа фандинг рейтов за последние {args.days} дней")
        
        # Проверяем наличие API ключей
        if not BYBIT_API_KEY or not BYBIT_API_SECRET:
            logger.error("Не установлены API ключи Bybit. Проверьте .env файл")
            return
        
        # Сбор данных
        historical_data = await collect_data(days=args.days)
        
        if historical_data.empty:
            logger.error("Не удалось собрать данные о фандинг рейтах")
            return
        
        logger.info(f"Собрано {len(historical_data)} записей о фандинг рейтах")
        
        if not args.collect_only:
            # Анализ данных
            await analyze_data(historical_data)
        else:
            logger.info("Сбор данных завершен. Анализ пропущен из-за флага --collect-only")
        
    except Exception as e:
        logger.error(f"Ошибка в главной функции: {e}")

if __name__ == "__main__":
    asyncio.run(main())