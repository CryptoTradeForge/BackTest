from typing import Optional, List, Dict, Union, Any
from pathlib import Path
import csv

class BackTestFutures:
    
    def __init__(
        self, 
        initial_balance: float = 1000.0,
        profit_record_folder = "BackTest/profit_record"
    ):
        """
        Initializes the BackTestFutures with an initial balance and a path to save profit records.
        
        Args:
            initial_balance (float): 初始餘額
            profit_record_path (str): 儲存利潤記錄的路徑
        """
        self.balance = initial_balance
        
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
        print(f"Profit record will be saved to: {self.profit_record_path}")
        
        # clean profit_rec_path and add header
        with open(self.profit_record_path, 'w', newline='') as f:
            writer = csv.writer(f)
            # writer.writerow(["symbol", "type", "order_time", "entry_time", "entry_price", "exit_time", "exit_price", "amount", "leverage", "result", "pnl", "pnl_pct", "win/loss"])
            writer.writerow(["symbol", "type", "entry_time", "entry_price", "exit_time", "exit_price", "exit_result", "amount", "leverage", "pnl", "pnl_pct", "win/loss"])
        
        self.data = None # 開始回測時要先 update 資料
        '''
        data 形式：{
            timestamp: xxxxxxxxx, 
            symbol1: {
                open: xxxxxxx,
                high: xxxxxxxx,
                low: xxxxxxxx,
            },
            symbol2: {
                open: xxxxxxx,
                high: xxxxxxxx,
                low: xxxxxxxx,
            },
        }
        '''
        
        self.opening_positions = []
        
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
            amount (float): 交易金額 (USDT)
            stop_loss_price (float, optional): 止損價格
            take_profit_price (float, optional): 止盈價格
        """
        pass
    
    def close_position(self, symbol: str, position_type: str) -> None:
        """
        平倉指定倉位
        
        Args:
            symbol (str): 交易對名稱
            position_type (str): 倉位類型 ("LONG"/"SHORT")
        """
        pass
    
    def set_stop_loss_take_profit(self, symbol: str, side: str, amount: float, 
                                 stop_loss_price: Optional[float] = None, 
                                 take_profit_price: Optional[float] = None) -> None:
        """
        設置止損和止盈
        
        Args:
            symbol (str): 交易對名稱
            side (str): 倉位方向
            amount (float): 交易金額 (USDT)
            stop_loss_price (float, optional): 止損價格
            take_profit_price (float, optional): 止盈價格
        """
        pass
    
    
    def get_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        獲取持倉資訊
        
        Args:
            symbol (str, optional): 交易對名稱，若不指定則獲取所有持倉
        Returns:
            list: 持倉資訊
        """
        pass
    
    def fetch_usdt_balance(self) -> Dict[str, float]:
        """
        獲取USDT餘額
        
        Returns:
            dict: 包含可用餘額、已用餘額和總餘額的字典
        """
        pass

    def get_price(self, symbol: str) -> float:
        """
        獲取當前價格
        
        Args:
            symbol (str): 交易對名稱
        Returns:
            float: 當前價格
        """
        pass
    
    def fetch_usdt_balance(self) -> Dict[str, float]:
        """
        獲取USDT餘額
        
        Returns:
            dict: 包含可用餘額、已用餘額和總餘額的字典
        """
        pass
    
    # -------------------- Backtesting Methods --------------------
    def update_data(self):
        pass
    
    def check_stop_loss_take_profit(self):
        pass