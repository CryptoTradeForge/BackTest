

class BackDataAcquisition:
    """
    A class to handle the acquisition of backtest data.
    This class is responsible for fetching and managing historical data for backtesting purposes.
    """


    def acquire_data(self, data, timestamp, limit=1000):
        """
        Acquire historical data for backtesting.

        Args:
            data (dict): The historical data to be acquired.
            timestamp (str or int): The timestamp for the data.
            limit (int): The maximum number of data points to acquire.

        Returns:
        
        """
        
        found_data = {}
        current_data = {}
        
        for symbol, symbol_data in data.items():
            for timeframe, timeframe_data in symbol_data.items():
                
                final_timestamp_idx_before_time_line = 0
                for i, (ts, price) in enumerate(timeframe_data):
                    if ts <= timestamp:
                        final_timestamp_idx_before_time_line = i
                    else:
                        break
                
                # the last data is the current data
                if final_timestamp_idx_before_time_line >= 0:
                    current_data[symbol] = {
                        "timestamp": timeframe_data[final_timestamp_idx_before_time_line][0],
                        "price": timeframe_data[final_timestamp_idx_before_time_line][1]
                    }
                
        