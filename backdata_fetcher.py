# -*- coding: utf-8 -*-
import pytz

class BackDataFetcher:
    """
    A class to fetch backtest data.
    """

    def __init__(self, future_api, top_cryptos_api, save_folder = 'backtest_data'):
        """
        Initializes the BackDataFetcher with a future API and a top cryptocurrencies API.
        :param future_api: An instance of the future API to fetch historical data.
        :param top_cryptos_api: An instance of the top cryptocurrencies API to fetch top symbols.
        :param save_folder: The folder where the fetched data will be saved.
        """
        self.future_api = future_api
        self.top_cryptos_api = top_cryptos_api
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
        return all_data



    def fetch_topk_data(self, topk: int = 10, limit: int = 10000, buffer: int = 1000) -> dict:
        """
        Fetches historical data for the top k cryptocurrencies.
        :param topk: The number of top cryptocurrencies to fetch data for.
        :param limit: The maximum number of data points to fetch for the shortest timeframe.
        :param buffer: Additional data points to fetch for longer timeframes to ensure sufficient data.
        :return: A dictionary containing historical data for the top k cryptocurrencies across different timeframes.
        """
        symbols = self.top_cryptos_api.get_top_cryptos(limit=topk)
        return self.fetch_data_symbols(symbols, limit, buffer)





if __name__ == "__main__":
    # Example usage
    from CryptoAPI.futures.binance_api import BinanceFutures
    from CryptoAPI.cmc_api import CoinMarketCapAPI
    future_api = BinanceFutures()
    top_cryptos_api = CoinMarketCapAPI()
    fetcher = BackDataFetcher(future_api, top_cryptos_api)

    data = fetcher.fetch_topk_data(topk=10, limit=10, buffer=1)
    print(data.keys())  