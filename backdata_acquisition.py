# File: backdata_acquisition.py

# 預設最小 timeframe 為 5 分鐘 (用於 current data)
# current data 預設只取 open, high, low 三個欄位 (可以自己改)

class BackDataAcquisition:
    """
    A class to handle the acquisition of backtest data.
    This class is responsible for fetching and managing historical data for backtesting purposes.
    Uses incremental indexing (two-pointer technique) for efficient timestamp lookups during sequential backtesting.
    """
    
    # Field mapping for different data fields
    FIELD_MAP = {
        "open": 1,
        "high": 2,
        "low": 3,
        "close": 4,
        "volume": 5
    }
    
    def __init__(self, tf_to_ms, min_timeframe="5m", fields=("open", "high", "low")):
        """
        Initialize the BackDataAcquisition class.

        Args:
            min_timeframe (str): The minimum timeframe for the data acquisition, default is "5m".
            fields (tuple): The fields to extract for current_data, default is ("open", "high", "low").
                           Available fields: "open", "high", "low", "close", "volume"
        """
        self.tf_to_ms = tf_to_ms
        self.min_timeframe = min_timeframe
        self.fields = fields
        # Cache for incremental indexing - stores last found index for each symbol and timeframe
        self._index_cache = {}
    
    def _validate_fields(self, fields):
        """
        Validate that all requested fields are available in FIELD_MAP.
        
        Args:
            fields (tuple): Fields to validate
            
        Raises:
            ValueError: If any field is not supported
        """
        for field in fields:
            if field not in self.FIELD_MAP:
                raise ValueError(f"Field '{field}' not supported. Available fields: {list(self.FIELD_MAP.keys())}")
    
    def _is_candle_closed(self, candle_timestamp, timeframe, current_timestamp):
        """
        Check if a candle is closed at the current timestamp.
        
        Args:
            candle_timestamp (int): The timestamp of the candle (start time)
            timeframe (str): The timeframe of the candle
            current_timestamp (int): The current timestamp
            
        Returns:
            bool: True if candle is closed, False otherwise
        """
        if timeframe not in self.tf_to_ms:
            print(f"Warning: Unsupported timeframe '{timeframe}' not found in tf_to_ms")
            return False
        
        timeframe_ms = self.tf_to_ms[timeframe]
        candle_close_time = candle_timestamp + timeframe_ms
        return current_timestamp >= candle_close_time
    
    def _find_current_candle_index(self, timeframe_data, timestamp, timeframe):
        """
        Find the index of the current candle (may be open) at target timestamp.
        
        Args:
            timeframe_data (list): List of data points [(timestamp, open, high, low, close), ...]
            timestamp (int): Target timestamp
            timeframe (str): The timeframe of the data
            
        Returns:
            int: Index of the current candle at target timestamp, -1 if none found
        """
        if not timeframe_data:
            return -1
            
        if timeframe not in self.tf_to_ms:
            print(f"Warning: Unsupported timeframe '{timeframe}' not found in tf_to_ms")
            return -1
            
        timeframe_ms = self.tf_to_ms[timeframe]
        
        # Find the candle that contains the target timestamp
        # If timestamp is exactly at candle close time, it belongs to that candle
        for i in range(len(timeframe_data) - 1, -1, -1):  # Search backwards
            candle_start = timeframe_data[i][0]
            candle_end = candle_start + timeframe_ms
            
            if candle_start <= timestamp <= candle_end:
                return i
                
        return -1
    
    def _binary_search_timestamp(self, timeframe_data, timestamp, timeframe):
        """
        Binary search to find the index of the last closed candle at target timestamp.
        
        Args:
            timeframe_data (list): List of data points [(timestamp, open, high, low, close), ...]
            timestamp (int): Target timestamp
            timeframe (str): The timeframe of the data
            
        Returns:
            int: Index of the last closed candle at target timestamp, -1 if none found
        """
        if not timeframe_data:
            return -1
            
        left, right = 0, len(timeframe_data) - 1
        result = -1
        
        while left <= right:
            mid = (left + right) // 2
            if self._is_candle_closed(timeframe_data[mid][0], timeframe, timestamp):
                result = mid
                left = mid + 1
            else:
                right = mid - 1
                
        return result
    
    def _incremental_search_timestamp(self, timeframe_data, timestamp, cache_key, timeframe):
        """
        Incremental search using cached index for efficient sequential timestamp lookups.
        Only returns closed candles.
        
        Args:
            timeframe_data (list): List of data points [(timestamp, open, high, low, close), ...]
            timestamp (int): Target timestamp
            cache_key (str): Key for index caching
            timeframe (str): The timeframe of the data
            
        Returns:
            int: Index of the last closed candle at target timestamp, -1 if none found
        """
        if not timeframe_data:
            return -1
            
        # Get cached index, default to 0 if not found
        start_idx = self._index_cache.get(cache_key, 0)
        
        # Ensure start_idx is within bounds
        start_idx = max(0, min(start_idx, len(timeframe_data) - 1))
        
        # Check if we need to search backwards (timestamp went back in time)
        if start_idx > 0 and timeframe_data[start_idx][0] > timestamp:
            # Fall back to binary search for backwards lookup
            result = self._binary_search_timestamp(timeframe_data, timestamp, timeframe)
        else:
            # Forward incremental search
            result = -1
            for i in range(start_idx, len(timeframe_data)):
                if self._is_candle_closed(timeframe_data[i][0], timeframe, timestamp):
                    result = i
                else:
                    break
        
        # Update cache with the found index
        if result >= 0:
            self._index_cache[cache_key] = result
            
        return result
    
    def _find_timestamp_index(self, timeframe_data, timestamp, cache_key, timeframe, force_binary_search=False):
        """
        Find the index of the last closed candle at target timestamp.
        
        Args:
            timeframe_data (list): List of data points [(timestamp, open, high, low, close), ...]
            timestamp (int): Target timestamp
            cache_key (str): Key for index caching
            timeframe (str): The timeframe of the data
            force_binary_search (bool): If True, use binary search instead of incremental search
            
        Returns:
            int: Index of the last closed candle at target timestamp, -1 if none found
        """
        if force_binary_search:
            return self._binary_search_timestamp(timeframe_data, timestamp, timeframe)
        else:
            return self._incremental_search_timestamp(timeframe_data, timestamp, cache_key, timeframe)
    
    def _extract_current_data_fields(self, data_point, fields):
        """
        Extract specified fields from a data point for current_data.
        
        Args:
            data_point (tuple): Data point (timestamp, open, high, low, close, ...)
            fields (tuple): Fields to extract
            
        Returns:
            dict: Dictionary with extracted field values
        """
        result = {}
        for field in fields:
            if field in self.FIELD_MAP:
                field_idx = self.FIELD_MAP[field]
                if field_idx < len(data_point):
                    result[field] = float(data_point[field_idx])
        return result
    
    def acquire_single_symbol_data(self, symbol, data, timestamp, limit=None, force_binary_search=False):
        """
        Acquire historical data for a single symbol for backtesting.
        Uses incremental indexing for efficient sequential timestamp lookups.

        Args:
            symbol (str): The trading symbol (used for caching).
            data (dict): The historical data for a single symbol.
            
            data should be structured as follows:
            {
                "5m": [(timestamp1, open1, high1, low1, close1), ...],    (timestamps should be increasing)
                "15m": [(timestamp2, open2, high2, low2, close2), ...],
                ...
            }
            
            timestamp (str or int): The timestamp for the data.
            limit (int, optional): The maximum number of historical data points to acquire.
                                 If None, returns all data from the beginning to timestamp.
            force_binary_search (bool): If True, use binary search instead of incremental search.
                                      Default is False for optimal performance during sequential backtesting.
            
        Returns:
            tuple: A tuple containing two dictionaries:
                - current_data: The current data for the symbol at the specified timestamp.
                              Contains timestamp and specified fields (open, high, low by default).
                - fetched_data: The historical data fetched for the symbol and timeframes (excluding current data).
                              Limited by the 'limit' parameter for each timeframe, or all data if limit is None.
        """
        # Validate fields
        self._validate_fields(self.fields)
        
        fetched_data = {}
        current_data = {"timestamp": timestamp}
        
        for timeframe, timeframe_data in data.items():
            cache_key = f"single_{symbol}_{timeframe}"
            
            # Find the last closed candle at target timestamp for historical data
            final_timestamp_idx = self._find_timestamp_index(
                timeframe_data, timestamp, cache_key, timeframe, force_binary_search
            )
            
            # Warning if no valid data found for this timeframe
            if final_timestamp_idx < 0:
                print(f"Warning: No closed candles found for symbol '{symbol}' timeframe '{timeframe}' at timestamp {timestamp}")
            
            # Extract current data for minimum timeframe (find current candle, may be open)
            if timeframe == self.min_timeframe:
                current_candle_idx = self._find_current_candle_index(timeframe_data, timestamp, timeframe)
                if current_candle_idx >= 0:
                    current_fields = self._extract_current_data_fields(
                        timeframe_data[current_candle_idx], self.fields
                    )
                    current_data.update(current_fields)
            
            # Fetch historical data (only closed candles)
            if final_timestamp_idx >= 0:
                if limit is None:
                    # Return all closed data from beginning to timestamp
                    start_idx = 0
                else:
                    # Return limited data based on limit parameter
                    start_idx = max(0, final_timestamp_idx - limit + 1)
                end_idx = final_timestamp_idx + 1
                fetched_data[timeframe] = timeframe_data[start_idx:end_idx]
            else:
                fetched_data[timeframe] = []
        
        return current_data, fetched_data
    
    def acquire_data(self, data, timestamp, limit=None, force_binary_search=False):
        """
        Acquire historical data for multiple symbols for backtesting.
        Uses incremental indexing for efficient sequential timestamp lookups.

        Args:
            data (dict): The historical data to be acquired for multiple symbols.
            
            data should be structured as follows:
            {
                "symbol1": {
                    "5m": [(timestamp1, open1, high1, low1, close1), ...],    (timestamps should be increasing)
                    "15m": [(timestamp2, open2, high2, low2, close2), ...],
                    ...
                },
                "symbol2": {
                    ...
                },
                ...
            }
            
            timestamp (str or int): The timestamp for the data.
            limit (int, optional): The maximum number of historical data points to acquire per symbol per timeframe.
                                 If None, returns all data from the beginning to timestamp.
            force_binary_search (bool): If True, use binary search instead of incremental search.
                                      Default is False for optimal performance during sequential backtesting.
            
        Returns:
            tuple: A tuple containing two dictionaries:
                - current_data: The current data for each symbol at the specified timestamp.
                              Contains timestamp and for each symbol: specified fields (open, high, low by default).
                - fetched_data: The historical data fetched for each symbol and timeframe (excluding current data).
                              Limited by the 'limit' parameter for each symbol and timeframe, or all data if limit is None.
        """
        # Validate fields
        self._validate_fields(self.fields)
        
        fetched_data = {}
        current_data = {"timestamp": timestamp}
        
        for symbol, symbol_data in data.items():
            fetched_data[symbol] = {}
            
            for timeframe, timeframe_data in symbol_data.items():
                cache_key = f"{symbol}_{timeframe}"
                
                # Find the last closed candle at target timestamp for historical data
                final_timestamp_idx = self._find_timestamp_index(
                    timeframe_data, timestamp, cache_key, timeframe, force_binary_search
                )
                
                # Warning if no valid data found for this timeframe
                if final_timestamp_idx < 0:
                    print(f"Warning: No closed candles found for symbol '{symbol}' timeframe '{timeframe}' at timestamp {timestamp}")
                
                # Extract current data for minimum timeframe (find current candle, may be open)
                if timeframe == self.min_timeframe:
                    current_candle_idx = self._find_current_candle_index(timeframe_data, timestamp, timeframe)
                    if current_candle_idx >= 0:
                        current_fields = self._extract_current_data_fields(
                            timeframe_data[current_candle_idx], self.fields
                        )
                        current_data[symbol] = current_fields
                
                # Fetch historical data (only closed candles)
                if final_timestamp_idx >= 0:
                    if limit is None:
                        # Return all closed data from beginning to timestamp
                        start_idx = 0
                    else:
                        # Return limited data based on limit parameter
                        start_idx = max(0, final_timestamp_idx - limit + 1)
                    end_idx = final_timestamp_idx + 1
                    fetched_data[symbol][timeframe] = timeframe_data[start_idx:end_idx]
                else:
                    fetched_data[symbol][timeframe] = []
        
        return current_data, fetched_data
    
    def reset_cache(self):
        """
        Reset the index cache. Useful when starting a new backtesting session
        or when timestamp sequence is reset.
        """
        self._index_cache.clear()
    
    def get_cache_info(self):
        """
        Get information about the current index cache state.
        
        Returns:
            dict: Current cache state with keys and their cached indices
        """
        return self._index_cache.copy()