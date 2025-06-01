# -*- coding: utf-8 -*-
import pytz
import json
from datetime import datetime
from pathlib import Path


class BackDataFetcher:
    """
    A class to fetch backtest data.
    """

    def __init__(self, future_api, cmc_api, save_folder = 'BackTest/back_data'):
        """
        Initializes the BackDataFetcher with the necessary APIs and save folder.
        :param future_api: An instance of the futures API to fetch historical data.
        :param cmc_api: An instance of the CoinMarketCap API to fetch top cryptocurrencies.
        :param save_folder: The folder where the fetched data will be saved.
        """
        
        self.future_api = future_api
        self.cmc_api = cmc_api
        self.save_folder = save_folder
        self.timezone = pytz.timezone("Asia/Taipei")

    
    def fetch_data(self, symbol: str, limit: int = 10000, buffer: int = 1000) -> dict:
        """
        Fetches historical data for a given symbol across multiple timeframes.
        :param symbol: The trading symbol for which to fetch data.
        :param limit: The maximum number of data points to fetch for the shortest timeframe.
        :param buffer: Additional data points to fetch for longer timeframes to ensure sufficient data.
        :return: A dictionary containing historical data for the symbol across different timeframes.
        """
        data = {
            '5m': self.future_api.get_historical_data(symbol=symbol, interval='5m', limit=limit),
            '15m': self.future_api.get_historical_data(symbol=symbol, interval='15m', limit=limit // 3 + buffer + 5),
            '1h': self.future_api.get_historical_data(symbol=symbol, interval='1h', limit=limit // 12 + buffer + 5),
            '4h': self.future_api.get_historical_data(symbol=symbol, interval='4h', limit=limit // 48 + buffer + 5),
            '1d': self.future_api.get_historical_data(symbol=symbol, interval='1d', limit=limit // 288 + buffer + 5)
        }
        return data



    def fetch_data_symbols(self, symbols: list, limit: int = 10000, buffer: int = 1000) -> dict:
        """
        Fetches historical data for multiple symbols across different timeframes.
        :param symbols: A list of trading symbols for which to fetch data.
        :param limit: The maximum number of data points to fetch for the shortest timeframe.
        :param buffer: Additional data points to fetch for longer timeframes to ensure sufficient data.
        :return: A dictionary containing historical data for each symbol across different timeframes.
        """
        all_data = {}
        for symbol in symbols:
            try:
                all_data[symbol] = self.fetch_data(symbol, limit, buffer)
            except Exception as e:
                print(f"Error fetching data for {symbol}: {e}")
                self.cmc_api.add_problematic_coin(symbol)
        return all_data

    def fetch_topk_data(self, topk: int = 10, limit: int = 10000, buffer: int = 1000) -> dict:
        """
        Fetches historical data for the top k cryptocurrencies.
        :param topk: The number of top cryptocurrencies to fetch data for.
        :param limit: The maximum number of data points to fetch for the shortest timeframe.
        :param buffer: Additional data points to fetch for longer timeframes to ensure sufficient data.
        :return: A dictionary containing historical data for the top k cryptocurrencies across different timeframes.
        """
        symbols = self.cmc_api.get_top_cryptos(limit=topk)
        return self.fetch_data_symbols(symbols, limit, buffer)

    def fetch_topk_and_save(self, topk: int = 10, limit: int = 10000, buffer: int = 1000, save_path: str = None) -> dict:
        """
        Fetches historical data for the top k cryptocurrencies and saves it to a specified path.
        :param topk: The number of top cryptocurrencies to fetch data for.
        :param limit: The maximum number of data points to fetch for the shortest timeframe.
        :param buffer: Additional data points to fetch for longer timeframes to ensure sufficient data.
        :param save_path: The path where the fetched data will be saved. If None, uses the default save folder.
        :return: A dictionary containing historical data for the top k cryptocurrencies across different timeframes.
        """
        if save_path is None:
            save_path = self.save_folder
        all_data = self.fetch_topk_data(topk, limit, buffer)
        
        now = datetime.now(self.timezone).strftime("%Y%m%d_%H%M")
        filename = f"{save_path}/top_{topk}_data_{now}.json"
        
        Path(save_path).mkdir(parents=True, exist_ok=True)
        with open(filename, 'w') as file:
            json.dump(all_data, file)
        
        print(f"Data saved to {filename}")
        return all_data

if __name__ == "__main__":
    # Example usage
    from CryptoAPI.futures.binance_api import BinanceFutures
    from CryptoAPI.cmc_api import CoinMarketCapAPI
    future_api = BinanceFutures()
    cmc_api = CoinMarketCapAPI()
    fetcher = BackDataFetcher(future_api, cmc_api)

    data = fetcher.fetch_topk_and_save(topk=10, limit=10, buffer=1)
    print(data.keys())