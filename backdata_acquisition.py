# File: backdata_acquisition.py

# 預設最小 timeframe 為 5 分鐘 (用於 current data)
# current data 預設只取 open, high, low 三個欄位 (可以自己改)

# 2025/08/31
# 主要改進：優化 _find_current_candle_index 使用二分搜尋，修復邊界條件問題

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
        Initialize the optimized BackDataAcquisition class.

        Args:
            tf_to_ms (dict): Timeframe to milliseconds mapping
            min_timeframe (str): The minimum timeframe for the data acquisition, default is "5m".
            fields (tuple): The fields to extract for current_data, default is ("open", "high", "low").
        """
        self.tf_to_ms = tf_to_ms
        self.min_timeframe = min_timeframe
        self.fields = fields
        # Cache for incremental indexing
        self._index_cache = {}
        # Cache timeframe_ms to avoid repeated dictionary lookups
        self._timeframe_ms_cache = {}
    
    def _get_timeframe_ms(self, timeframe):
        """Get timeframe milliseconds with caching."""
        if timeframe not in self._timeframe_ms_cache:
            if timeframe not in self.tf_to_ms:
                raise ValueError(f"Unsupported timeframe '{timeframe}' not found in tf_to_ms")
            self._timeframe_ms_cache[timeframe] = self.tf_to_ms[timeframe]
        return self._timeframe_ms_cache[timeframe]
    
    def _validate_fields(self, fields):
        """Validate that all requested fields are available in FIELD_MAP."""
        for field in fields:
            if field not in self.FIELD_MAP:
                raise ValueError(f"Field '{field}' not supported. Available fields: {list(self.FIELD_MAP.keys())}")
    
    def _is_candle_closed(self, candle_timestamp, timeframe, current_timestamp):
        """
        Check if a candle is closed at the current timestamp.
        Optimized with caching.
        """
        timeframe_ms = self._get_timeframe_ms(timeframe)
        candle_close_time = candle_timestamp + timeframe_ms
        return current_timestamp >= candle_close_time
    
    def _binary_search_closed_candles(self, timeframe_data, timestamp, timeframe):
        """
        Optimized binary search to find the last CLOSED candle.
        Fixes the bug where unclosed candles were returned.
        """
        if not timeframe_data:
            return -1
            
        timeframe_ms = self._get_timeframe_ms(timeframe)
        left, right = 0, len(timeframe_data) - 1
        result = -1
        
        while left <= right:
            mid = (left + right) // 2
            candle_timestamp = timeframe_data[mid][0]
            candle_close_time = candle_timestamp + timeframe_ms
            
            # 關鍵修復：檢查蠟燭是否已經關閉
            if candle_close_time <= timestamp:
                result = mid
                left = mid + 1
            else:
                right = mid - 1
                
        return result
    
    def _binary_search_current_candle(self, timeframe_data, timestamp, timeframe):
        """
        Optimized binary search to find the current candle (may be open).
        When timestamp is exactly at candle boundary, prefer the newer candle to match new version.
        """
        if not timeframe_data:
            return -1
            
        timeframe_ms = self._get_timeframe_ms(timeframe)
        left, right = 0, len(timeframe_data) - 1
        
        # Find all matching candles first
        matching_indices = []
        
        # Standard binary search, but collect all matches
        while left <= right:
            mid = (left + right) // 2
            candle_start = timeframe_data[mid][0]
            candle_end = candle_start + timeframe_ms
            
            if candle_start <= timestamp <= candle_end:
                matching_indices.append(mid)
                # Check left side for more matches
                temp_left = mid - 1
                while temp_left >= 0:
                    temp_start = timeframe_data[temp_left][0]
                    temp_end = temp_start + timeframe_ms
                    if temp_start <= timestamp <= temp_end:
                        matching_indices.append(temp_left)
                        temp_left -= 1
                    else:
                        break
                
                # Check right side for more matches
                temp_right = mid + 1
                while temp_right < len(timeframe_data):
                    temp_start = timeframe_data[temp_right][0]
                    temp_end = temp_start + timeframe_ms
                    if temp_start <= timestamp <= temp_end:
                        matching_indices.append(temp_right)
                        temp_right += 1
                    else:
                        break
                
                # Return the highest index (newest candle) to match new version behavior
                return max(matching_indices)
                
            elif timestamp < candle_start:
                right = mid - 1
            else:  # timestamp > candle_end
                left = mid + 1
                
        return -1
    
    def _incremental_search_closed_candles(self, timeframe_data, timestamp, cache_key, timeframe):
        """
        Optimized incremental search for closed candles only.
        """
        if not timeframe_data:
            return -1
            
        timeframe_ms = self._get_timeframe_ms(timeframe)
        start_idx = self._index_cache.get(cache_key, 0)
        start_idx = max(0, min(start_idx, len(timeframe_data) - 1))
        
        # Check if we need to search backwards
        if start_idx > 0:
            candle_close_time = timeframe_data[start_idx][0] + timeframe_ms
            if candle_close_time > timestamp:
                # Fall back to binary search for backwards lookup
                return self._binary_search_closed_candles(timeframe_data, timestamp, timeframe)
        
        # Forward incremental search
        result = -1
        for i in range(start_idx, len(timeframe_data)):
            candle_timestamp = timeframe_data[i][0]
            candle_close_time = candle_timestamp + timeframe_ms
            
            if candle_close_time <= timestamp:
                result = i
            else:
                break
        
        # Update cache
        if result >= 0:
            self._index_cache[cache_key] = result
            
        return result
    
    def _find_closed_candle_index(self, timeframe_data, timestamp, cache_key, timeframe, force_binary_search=False):
        """Find the index of the last closed candle."""
        if force_binary_search:
            return self._binary_search_closed_candles(timeframe_data, timestamp, timeframe)
        else:
            return self._incremental_search_closed_candles(timeframe_data, timestamp, cache_key, timeframe)
    
    def _extract_current_data_fields(self, data_point, fields):
        """Extract specified fields from a data point for current_data."""
        result = {}
        for field in fields:
            if field in self.FIELD_MAP:
                field_idx = self.FIELD_MAP[field]
                if field_idx < len(data_point):
                    result[field] = float(data_point[field_idx])
        return result
    
    def acquire_single_symbol_data(self, symbol, data, timestamp, limit=None, force_binary_search=False):
        """
        Optimized version of acquire_single_symbol_data.
        future work: limit 可設定要隨 timeframes 調整還是固定。
        """
        self._validate_fields(self.fields)
        
        fetched_data = {}
        current_data = {"timestamp": timestamp}
        
        for timeframe, timeframe_data in data.items():
            cache_key = f"single_{symbol}_{timeframe}"
            
            # Find the last closed candle for historical data
            final_timestamp_idx = self._find_closed_candle_index(
                timeframe_data, timestamp, cache_key, timeframe, force_binary_search
            )
            
            # Extract current data for minimum timeframe (find current candle, may be open)
            if timeframe == self.min_timeframe:
                current_candle_idx = self._binary_search_current_candle(timeframe_data, timestamp, timeframe)
                if current_candle_idx >= 0:
                    current_fields = self._extract_current_data_fields(
                        timeframe_data[current_candle_idx], self.fields
                    )
                    current_data.update(current_fields)
            
            # Fetch historical data (only closed candles)
            if final_timestamp_idx >= 0:
                if limit is None:
                    start_idx = 0
                else:
                    start_idx = max(0, final_timestamp_idx - limit + 1)
                end_idx = final_timestamp_idx + 1
                fetched_data[timeframe] = timeframe_data[start_idx:end_idx]
            else:
                fetched_data[timeframe] = []
        
        return current_data, fetched_data
    
    def acquire_data(self, data, timestamp, limit=None, force_binary_search=False):
        """
        Optimized version of acquire_data.
        future work: limit 可設定要隨 timeframes 調整還是固定。
        """
        self._validate_fields(self.fields)
        
        fetched_data = {}
        current_data = {"timestamp": timestamp}
        
        for symbol, symbol_data in data.items():
            fetched_data[symbol] = {}
            
            for timeframe, timeframe_data in symbol_data.items():
                cache_key = f"{symbol}_{timeframe}"
                
                # Find the last closed candle for historical data
                final_timestamp_idx = self._find_closed_candle_index(
                    timeframe_data, timestamp, cache_key, timeframe, force_binary_search
                )
                
                # Extract current data for minimum timeframe (find current candle, may be open)
                if timeframe == self.min_timeframe:
                    current_candle_idx = self._binary_search_current_candle(timeframe_data, timestamp, timeframe)
                    if current_candle_idx >= 0:
                        current_fields = self._extract_current_data_fields(
                            timeframe_data[current_candle_idx], self.fields
                        )
                        current_data[symbol] = current_fields
                
                # Fetch historical data (only closed candles)
                if final_timestamp_idx >= 0:
                    if limit is None:
                        start_idx = 0
                    else:
                        start_idx = max(0, final_timestamp_idx - limit + 1)
                    end_idx = final_timestamp_idx + 1
                    fetched_data[symbol][timeframe] = timeframe_data[start_idx:end_idx]
                else:
                    fetched_data[symbol][timeframe] = []
        
        return current_data, fetched_data
    
    def reset_cache(self):
        """Reset the index cache."""
        self._index_cache.clear()
    
    def get_cache_info(self):
        """Get information about the current index cache state."""
        return self._index_cache.copy()
