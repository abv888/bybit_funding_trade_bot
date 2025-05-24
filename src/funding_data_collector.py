import asyncio
import datetime
import json
import logging
import os
import time
from typing import Dict, List, Optional

import pandas as pd

from bybit_client import BybitClient

logger = logging.getLogger(__name__)

class FundingDataCollector:
    """Класс для сбора и хранения исторических данных о фандинг рейтах"""
    
    def __init__(self, bybit_client: BybitClient, data_dir: str = "data"):
        """
        Инициализация коллектора данных
        
        Args:
            bybit_client: Экземпляр клиента Bybit
            data_dir: Директория для хранения данных
        """
        self.bybit = bybit_client
        self.data_dir = data_dir
        
        # Создаем директорию для данных, если она не существует
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(os.path.join(data_dir, "funding_rates"), exist_ok=True)
        
    async def collect_current_funding_rates(self) -> pd.DataFrame:
        """
        Сбор текущих фандинг рейтов
        
        Returns:
            pd.DataFrame: Датафрейм с текущими фандинг рейтами
        """
        try:
            funding_df = await self.bybit.get_funding_rates()
            
            if not funding_df.empty:
                # Добавляем временную метку сбора данных
                funding_df["collected_at"] = datetime.datetime.now()
                
                # Сохраняем данные
                self._save_funding_data(funding_df)
                
                logger.info(f"Собрано {len(funding_df)} записей о фандинг рейтах")
                
            return funding_df
        
        except Exception as e:
            logger.error(f"Ошибка при сборе фандинг рейтов: {e}")
            return pd.DataFrame()
    
    def _save_funding_data(self, funding_df: pd.DataFrame) -> None:
        """
        Сохранение данных о фандинг рейтах
        
        Args:
            funding_df: Датафрейм с данными о фандинг рейтах
        """
        try:
            if funding_df.empty:
                return
            
            # Имя файла на основе текущей даты
            date_str = datetime.datetime.now().strftime("%Y-%m-%d")
            file_path = os.path.join(self.data_dir, "funding_rates", f"{date_str}.csv")
            
            # Проверяем, существует ли файл
            if os.path.exists(file_path):
                # Если файл существует, дописываем данные
                existing_df = pd.read_csv(file_path)
                combined_df = pd.concat([existing_df, funding_df], ignore_index=True)
                combined_df.to_csv(file_path, index=False)
            else:
                # Если файл не существует, создаем новый
                funding_df.to_csv(file_path, index=False)
                
            logger.info(f"Данные о фандинг рейтах сохранены в {file_path}")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных о фандинг рейтах: {e}")
    
    def load_historical_funding_data(self, days: int = 7) -> pd.DataFrame:
        """
        Загрузка исторических данных о фандинг рейтах
        
        Args:
            days: Количество дней для загрузки (по умолчанию 7 дней)
            
        Returns:
            pd.DataFrame: Датафрейм с историческими данными
        """
        try:
            all_data = []
            
            # Получаем список дат
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=days)
            
            current_date = start_date
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                file_path = os.path.join(self.data_dir, "funding_rates", f"{date_str}.csv")
                
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    all_data.append(df)
                
                current_date += datetime.timedelta(days=1)
            
            if all_data:
                # Объединяем все данные
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # Удаляем дубликаты
                combined_df.drop_duplicates(inplace=True)
                
                logger.info(f"Загружено {len(combined_df)} записей исторических данных о фандинг рейтах")
                
                return combined_df
            else:
                logger.warning("Не найдено исторических данных о фандинг рейтах")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Ошибка при загрузке исторических данных о фандинг рейтах: {e}")
            return pd.DataFrame()
    
    async def start_collection(self, interval_minutes: int = 60) -> None:
        """
        Запуск периодического сбора данных
        
        Args:
            interval_minutes: Интервал сбора данных в минутах
        """
        logger.info(f"Запущен сбор данных о фандинг рейтах с интервалом {interval_minutes} минут")
        
        while True:
            try:
                await self.collect_current_funding_rates()
                
                # Ждем указанный интервал
                await asyncio.sleep(interval_minutes * 60)
                
            except Exception as e:
                logger.error(f"Ошибка в цикле сбора данных: {e}")
                await asyncio.sleep(60)  # Ждем минуту в случае ошибки