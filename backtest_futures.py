# 目前策略暫時用不到限價，所以只寫了市價的方法，之後如果需要再補上限價的部分
# -*- coding: utf-8 -*-

from typing import Optional, List, Dict, Union, Any
from pathlib import Path
import csv
import pytz
from datetime import datetime

class BackTestFutures:
    
    def __init__(
        self, 
        initial_balance: float = 1000.0,
        profit_record_path: str = None,
        profit_record_folder = "data/backtest_profit_record",
        show_info: bool = False
    ):
        """
        Initializes the BackTestFutures with an initial balance and a path to save profit records.
        
        Args:
            initial_balance (float): 初始餘額
            profit_record_path (str): 儲存利潤記錄的路徑
        """
        self.use_balance = True  # 是否使用餘額進行交易
        self.balance = initial_balance
        self.using_balance = 0
        self.timezone = pytz.timezone("Asia/Taipei")
        self.show_info = show_info
        
        if profit_record_path is not None:
            self.profit_record_path = Path(profit_record_path)
            self.profit_record_path.parent.mkdir(parents=True, exist_ok=True)
            if not self.profit_record_path.exists():
                self.need_initialize = True
            else:
                self.need_initialize = False
        else:
            # 儲存路徑： "{profit_record_folder}/profits_{i}.csv"
            # 如果編號 i 的檔案已存在，則會自動增加編號
            self.profit_record_folder = Path(profit_record_folder)
            self.profit_record_folder.mkdir(parents=True, exist_ok=True)
            
            i = 0
            while True:
                profit_record_path = self.profit_record_folder / f"profits_{i}.csv"
                if not profit_record_path.exists():
                    self.profit_record_path = profit_record_path
                    break
                i += 1

            self.profit_record_path = profit_record_path
            self.need_initialize = True
        
        if self.show_info:
            print(f"Profit record will be saved to: {self.profit_record_path}")
        

        self.data = None # 開始回測時要先 update 資料
        self.back_data = None # 用來存放回測資料
        '''
        data 形式：{
            timestamp: xxxxxxxxx,   # 時間戳記 (str or int)
            symbol1: {              
                open: xxxxxxx,      # 用來記錄開單/平倉時的價格 (float)
                high: xxxxxxxx,     # 拿來判斷 止盈/止損/爆倉 是否觸發 (float)
                low: xxxxxxxx,     # 拿來判斷 止盈/止損/爆倉 是否觸發 (float)
            },
            symbol2: {
                open: xxxxxxx,
                high: xxxxxxxx,
                low: xxxxxxxx,
            },
        }
        '''
        
        self.now = None # 用來記錄當前時間，回測時會更新
        self.opening_positions = []
    
    
    def set_show_info(self, show_info: bool) -> None:
        """
        設定是否顯示交易資訊
        
        Args:
            show_info (bool): 是否顯示交易資訊
        """
        self.show_info = show_info
        if self.show_info:
            print("交易資訊顯示已啟用")
    
    def set_use_balance(self, use_balance: bool) -> None:
        """
        設定是否使用餘額進行交易
        
        Args:
            use_balance (bool): 是否使用餘額進行交易
        """
        self.use_balance = use_balance
        
        # 如果不使用餘額，則清空已使用餘額
        if not self.use_balance:
            self.using_balance = 0
            self.balance = 999999999999  # 設定一個很大的餘額，避免在回測時因為餘額不足而無法開倉
            if self.show_info:
                print("[use_balance] is set to [False]")
        else:
            if self.show_info:
                print("[use_balance] is set to [True]")
        
    
    def import_opening_positions(self, positions: List[Dict[str, Any]]) -> None:
        """
        導入開倉資訊
        """
        
        if not isinstance(positions, list):
            raise ValueError("positions must be a list of dictionaries")
        
        # Validate all positions first
        for position in positions:
            if not isinstance(position, dict):
                raise ValueError("Each position must be a dictionary")
            
            required_keys = ["symbol", "position_type", "leverage", "amount", "entry_time", "entry_price"]
            for key in required_keys:
                if key not in position:
                    raise ValueError(f"Position is missing required key: {key}")
            
            self.opening_positions.append(position)
        
        if self.show_info:
            print(f"Imported {len(positions)} opening positions, current number of positions: {len(self.opening_positions)}")
    
    
    def initialize_profit_record(self):
        # clean profit_rec_path and add header
        with open(self.profit_record_path, 'w', newline='') as f:
            writer = csv.writer(f)
            # writer.writerow(["symbol", "position_type", "order_time", "entry_time", "entry_price", "exit_time", "exit_price", "amount", "leverage", "reason", "pnl", "pnl_pct", "win/loss"])
            writer.writerow(["symbol", "position_type", "entry_time", "entry_price", "exit_time", "exit_price", "exit_reason", "amount", "leverage", "pnl", "pnl_pct", "win/loss"])
        return False
            
    # -------------------- Futures Trading Methods --------------------
    def place_market_order(self, symbol: str, position_type: str, leverage: int, amount: float, 
                          stop_loss_price: Optional[float] = None, 
                          take_profit_price: Optional[float] = None) -> None:
        """
        市價開倉交易
        
        Args:
            symbol (str): 交易對名稱
            position_type (str): 倉位類型 ("LONG"/"SHORT")
            leverage (int): 槓桿倍數
            amount (float): 交易金額 (USDT) (包含槓桿)
            stop_loss_price (float, optional): 止損價格
            take_profit_price (float, optional): 止盈價格
        """
        
        # 檢查 止盈價格 和 止損價格 是否合理
        if stop_loss_price is not None:
            if position_type == "LONG" and stop_loss_price >= self.get_price(symbol):
                print(f"LONG position stop loss price: {stop_loss_price}, current price: {self.get_price(symbol)}")
                # raise ValueError("LONG position stop loss price must be lower than current price.")
                return
            elif position_type == "SHORT" and stop_loss_price <= self.get_price(symbol):
                print(f"SHORT position stop loss price: {stop_loss_price}, current price: {self.get_price(symbol)}")
                # raise ValueError("SHORT position stop loss price must be higher than current price.")
                return
                
        if take_profit_price is not None:
            if position_type == "LONG" and take_profit_price <= self.get_price(symbol):
                print(f"LONG position take profit price: {take_profit_price}, current price: {self.get_price(symbol)}")
                # raise ValueError("LONG position take profit price must be higher than current price.")
                return
            elif position_type == "SHORT" and take_profit_price >= self.get_price(symbol):
                print(f"SHORT position take profit price: {take_profit_price}, current price: {self.get_price(symbol)}")
                # raise ValueError("SHORT position take profit price must be lower than current price.")
                return
        
        
        # 確認餘額是否足夠
        if self.balance - self.using_balance < amount / leverage:
            if self.show_info:
                print(f"餘額不足，無法開 {position_type} 倉，交易對: {symbol}, 需要金額: {amount} USDT, 當前餘額: {self.balance} USDT")
            return
        
        price = self.get_price(symbol)
        if position_type not in ["LONG", "SHORT"]:
            raise ValueError("position_type must be 'LONG' or 'SHORT'")
        
        # 模擬開倉
        self.opening_positions.append({
            "symbol": symbol,
            "position_type": position_type,
            "leverage": leverage,
            "amount": amount,
            "entry_time": self.now,
            "entry_price": price,
            "stop_loss_price": stop_loss_price,
            "take_profit_price": take_profit_price
        })
        
        # 更新 using balance
        if self.use_balance:
            self.using_balance += amount / leverage
        
        if self.show_info:
            print(f"開 {position_type} 倉成功，交易對: {symbol}, 槓桿: {leverage}, 金額: {amount} USDT, 價格: {price}")
            
            if self.use_balance:
                print(f"當前餘額: {self.balance} USDT")
                print(f"當前使用餘額: {self.using_balance} USDT")
        
    
    def close_position(self, symbol: str, position_type: str, price: Optional[float] = None, 
                       exit_reason: str = "manual_close") -> None:
        """
        平倉指定倉位
        
        Args:
            symbol (str): 交易對名稱
            position_type (str): 倉位類型 ("LONG"/"SHORT")
            
            ---------- 下面兩個參數為回測專用，因為止損和止盈價格在回測時不會自動觸發，所以需要額外寫判斷 ----------
            
            price (float, optional): 平倉價格，若不指定則使用當前價格
            exit_reason (str): 平倉原因，預設為 "manual_close"
        """
        
        position = next((p for p in self.opening_positions if p["symbol"] == symbol and p["position_type"] == position_type), None)
        
        if not position:
            print(f"沒有找到 {symbol} 的 {position_type} 倉位，跳過平倉操作。")
            return
        
        # 模擬平倉
        exit_price = price if price is not None else self.get_price(symbol)
        
        pnl = (exit_price - position["entry_price"]) * (position["amount"] / position["entry_price"])
        
        pnl = pnl if position["position_type"] == "LONG" else -pnl  # 如果是 SHORT 倉位，PnL 需要反向計算
        pnl -= position["amount"] * 0.001 # 手續費
        
        pnl_pct = pnl / (position["amount"] / position["leverage"]) * 100
        
        if self.need_initialize:
            self.need_initialize = self.initialize_profit_record()
        
        # 記錄平倉資訊
        with open(self.profit_record_path, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                symbol, 
                position_type, 
                position["entry_time"], 
                position["entry_price"], 
                self.now, 
                exit_price, 
                exit_reason, 
                position["amount"], 
                position["leverage"], 
                pnl, 
                pnl_pct, 
                "win" if pnl > 0 else "loss"
            ])
        
        self.opening_positions.remove(position)
        
        # 更新餘額
        if self.use_balance:
            self.balance += pnl
            self.using_balance -= position["amount"] / position["leverage"]
        
        if self.show_info:
            print(f"平 {position_type} 倉成功，交易對: {symbol}, 平倉價格: {exit_price}, PnL: {pnl}, PnL%: {pnl_pct}%")
            if self.use_balance:
                print(f"當前餘額: {self.balance} USDT")
                print(f"當前使用餘額: {self.using_balance} USDT")
    
    def get_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        獲取持倉資訊
        
        Args:
            symbol (str, optional): 交易對名稱，若不指定則獲取所有持倉
        Returns:
            list: 持倉資訊
        """
        if symbol:
            return [p for p in self.opening_positions if p["symbol"] == symbol]
        return self.opening_positions
    
    def fetch_usdt_balance(self) -> Dict[str, float]:
        """
        獲取USDT餘額
        
        Returns:
            dict: 包含可用餘額、已用餘額和總餘額的字典
        """
        return {
            "available_balance": self.balance - self.using_balance,
            "used_balance": self.using_balance,
            "total_balance": self.balance
        }

    def get_price(self, symbol: str) -> float:
        """
        獲取當前價格
        
        Args:
            symbol (str): 交易對名稱
        Returns:
            float: 當前價格
        """
        
        # 從 self.data 中獲取當前價格
        if self.data is None:
            raise ValueError("Data not initialized. Please call update_data() first.")
        
        if symbol not in self.data:
            raise ValueError(f"Symbol {symbol} not found in data.")
        
        return self.data[symbol]["open"]
        
    
    def get_historical_data(
            self,
            symbol: str,
            interval: str,
            limit: Optional[int] = None,
            closed: bool = True,
            show: bool = False,
            since: Optional[int] = None
        ) -> List[List[Any]]:
        """
        獲取歷史K線數據

        Args:
            symbol (str): 交易對名稱
            interval (str): 時間間隔，如 "1m", "5m", "1h", "1d" 等
            limit (int, optional): 數量限制。若與 since 同時存在，則從 since 開始最多取 limit 根
            closed (bool, optional): 是否只獲取已關閉的K線。默認為True
            show (bool, optional): 是否顯示獲取進度。默認為False
            since (int, optional): 從這個時間戳（毫秒）開始取得K線資料

        Returns:
            list: K線數據列表

        Raises:
            Exception: 獲取歷史數據失敗時拋出異常
        """
        if self.back_data is None:
            raise ValueError("Data not initialized. Please call update_data() first.")
        
        if symbol not in self.back_data:
            raise ValueError(f"Symbol {symbol} not found in data.")
        
        if interval not in self.back_data[symbol]:
            raise ValueError(f"Interval {interval} not found for symbol {symbol}.")
        
        # 獲取指定時間間隔的數據
        historical_data = self.back_data[symbol][interval]
        if since is not None:
            # 如果指定了 since，則從 since 開始取數據
            historical_data = [data for data in historical_data if data[0] >= since]
            
        if limit is not None:
            # 如果指定了 limit，則只取最後 limit 根數據
            historical_data = historical_data[-limit:]
            
        if not closed:
            print("Warning: 'closed' parameter is not supported in backtesting mode. All data will be returned regardless of whether the candles are closed or not.")
        
        return historical_data
    
    def check_symbol_availability(self, symbol: str) -> bool:
        """
        檢查交易對是否可用
        
        Args:
            symbol (str): 交易對名稱
        Returns:
            bool: 如果交易對可用，返回True，否則返回False
        """
        if self.data is None:
            raise ValueError("Data not initialized. Please call update_data() first.")
        
        return symbol in self.data
    
    
    # -------------------- Backtesting Methods --------------------
    def update_data(self, data: Dict[str, Any], back_data: Optional[Dict[str, Any]] = None) -> None:
        """
        檢查止盈/止損/爆倉是否觸發，並更新回測資料。
        
        Args:
            data (dict): 回測資料，格式參見 self.data 的說明
        """
        
        # 更新資料
        self.data = data
        if back_data is not None:
            self.back_data = back_data
        
        timestamp = self.data["timestamp"] / 1000 # timestamp 是毫秒級別，要轉換為秒級別
        dt = datetime.fromtimestamp(timestamp, self.timezone)
        self.now = dt.strftime("%Y-%m-%d %H:%M:%S")
        
        # 檢查止盈/止損/爆倉是否觸發
        self.check_stop_loss_take_profit()
        
    def check_stop_loss_take_profit(self):
        """
        檢查所有開倉的止損和止盈以及爆倉是否觸發，若觸發則平倉。
        """
        
        for position in self.opening_positions:
            
            # 如果 position 剛開倉，則不檢查止盈/止損/爆倉
            if position["entry_time"] == self.now:
                if self.show_info:
                    print(f"Position for {position['symbol']} just opened, skipping stop loss/take profit check.")
                continue
            
            symbol = position["symbol"]
            position_type = position["position_type"]
            
            high = self.data[symbol]["high"]
            low = self.data[symbol]["low"]
            
            # 檢查爆倉
            if position_type == "LONG":
                
                # 檢查爆倉
                liquidation_price = position["entry_price"] * (1 - 1 / position["leverage"])
                if low <= liquidation_price:
                    self.close_position(symbol, position_type, price=liquidation_price, exit_reason="liquidation")
                    
                # 檢查止盈/止損
                elif position["stop_loss_price"] is not None and low <= position["stop_loss_price"]:
                    self.close_position(symbol, position_type, price=position["stop_loss_price"], exit_reason="stop_loss")
                elif position["take_profit_price"] is not None and high >= position["take_profit_price"]:
                    self.close_position(symbol, position_type, price=position["take_profit_price"], exit_reason="take_profit")

            elif position_type == "SHORT":
                
                # 檢查爆倉
                liquidation_price = position["entry_price"] * (1 + 1 / position["leverage"])
                if high >= liquidation_price:
                    self.close_position(symbol, position_type, price=liquidation_price, exit_reason="liquidation")
                    
                # 檢查止盈/止損
                elif position["stop_loss_price"] is not None and high >= position["stop_loss_price"]:
                    self.close_position(symbol, position_type, price=position["stop_loss_price"], exit_reason="stop_loss")
                elif position["take_profit_price"] is not None and low <= position["take_profit_price"]:
                    self.close_position(symbol, position_type, price=position["take_profit_price"], exit_reason="take_profit")