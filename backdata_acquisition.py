# 預設最小 timeframe 為 5 分鐘 (用於 current data)

class BackDataAcquisition:
    """
    A class to handle the acquisition of backtest data.
    This class is responsible for fetching and managing historical data for backtesting purposes.
    """
    
    
    def __init__(self, min_timeframe="5m"):
        """
        Initialize the BackDataAcquisition class.

        Args:
            min_timeframe (str): The minimum timeframe for the data acquisition, default is "5m".
        """
        self.min_timeframe = min_timeframe

    
    def acquire_data(self, data, timestamp, limit=1000):
        """
        Acquire historical data for backtesting.

        Args:
            data (dict): The historical data to be acquired.
            
            data should be structured as follows:
            {
                "symbol1": {
                    "5m": [(timestamp1, open1, high1, low1, close1), ...],    (timestamps wold be increasing)
                    "15m": [(timestamp2, open2, high2, low2, close2), ...],
                    ...
                },
                "symbol2": {
                    ...
                },
                ...
            }
            
            timestamp (str or int): The timestamp for the data.
            limit (int): The maximum number of data points to acquire.
            
        Returns:
            tuple: A tuple containing two dictionaries:
                - current_data: The current data for each symbol at the specified timestamp.
                - fetched_data: The historical data fetched for each symbol and timeframe.
        """
        
        fetched_data = {}
        current_data = {}
        
        current_data["timestamp"] = timestamp
        
        for symbol, symbol_data in data.items():
            fetched_data[symbol] = {}
            for timeframe, timeframe_data in symbol_data.items():
                
                final_timestamp_idx_before_time_line = 0
                for i, data_point in enumerate(timeframe_data):
                    ts = data_point[0]
                    if ts <= timestamp:
                        final_timestamp_idx_before_time_line = i
                    else:
                        break
                
                # the last data is the current data
                if timeframe == self.min_timeframe and final_timestamp_idx_before_time_line >= 0:
                    current_data[symbol] = {
                        "open": timeframe_data[final_timestamp_idx_before_time_line][1],
                        "high": timeframe_data[final_timestamp_idx_before_time_line][2],
                        "low": timeframe_data[final_timestamp_idx_before_time_line][3],
                        # 其他暫時用不到 (有需要的話可以再加上)
                    }
                
                # 不取 final_timestamp_idx_before_time_line (current data)
                # 往前取 limit 個數據 (也就是從 final_timestamp_idx_before_time_line - 1 那筆資料往前取)
                start_idx = max(0, final_timestamp_idx_before_time_line - limit)
                end_idx = final_timestamp_idx_before_time_line
                fetched_data[symbol][timeframe] = timeframe_data[start_idx:end_idx]
        
        
        return current_data, fetched_data
                
                
                
        