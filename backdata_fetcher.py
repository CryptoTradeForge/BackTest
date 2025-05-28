# -*- coding: utf-8 -*-


class BackDataFetcher:
    """
    A class to fetch backtest data.
    """

    def __init__(self, future_api, save_path = 'backtest_data'):
        """
        Initializes the BackDataFetcher with a future API and a save path.

        :param future_api: An instance of the future API to fetch data.
        :param save_path: The path where the fetched data will be saved.
        """
        self.future_api = future_api
        self.save_path = save_path
    
    def fetch_data_symbol(self, symbol: str, limit: int = 10000, buffer: int = 1000) -> dict:
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
















if __name__ == "__main__":
    # Example usage
    from CryptoAPI.futures.binance_api import BinanceFutures
    future_api = BinanceFutures()
    fetcher = BackDataFetcher(future_api)
    symbol = 'BTCUSDT'
    
    data = fetcher.fetch_data_symbol(symbol)
    print(data)