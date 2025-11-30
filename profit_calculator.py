import os
import csv
import glob
import re


class ProfitCalculator:
    
    # 將 all_profits 視覺化 (柱狀圖)
    @staticmethod
    def plot_all_profits(profits):
        import matplotlib.pyplot as plt
        plt.bar(range(len(profits)), profits)
        plt.title("All Profits")
        plt.xlabel("Trades")
        plt.ylabel("PnL")
        plt.show()
    
    
    # 將累計增減視覺化
    @staticmethod
    def plot_cumsum(cumsum):
        import matplotlib.pyplot as plt
        plt.plot(cumsum)
        plt.title("Cumulative PnL")
        plt.xlabel("Trades")
        plt.ylabel("PnL")
        plt.show()
    
    
    @staticmethod
    def calculate_tpwr(profits_detail):
        """
        計算 Total Profit and Win Rate (TPWR)
        :param profits_detail: 利潤詳細資料，應該是一個包含多個交易的列表，每個交易是一個字典
        :return: 一個字典，包含總利潤、勝率的統計數據
        """
        total_profit = sum(float(p["pnl"]) for p in profits_detail)
        total_trades = len(profits_detail)
        
        if total_trades == 0:
            return {"total_profit": 0, "win_rate": 0, "avg_win": 0, "avg_loss": 0}
        
        win_trade_list = [p for p in profits_detail if float(p["pnl"]) > 0]
        loss_trade_list = [p for p in profits_detail if float(p["pnl"]) < 0]
        
        win_trades = len(win_trade_list)
        win_rate = (win_trades / total_trades) * 100
        
        avg_win = sum(float(p["pnl"]) for p in win_trade_list) / win_trades if win_trades > 0 else 0
        avg_loss = sum(float(p["pnl"]) for p in loss_trade_list) / len(loss_trade_list) if loss_trade_list else 0
        
        return {
            "total_profit": round(total_profit, 3),
            "win_rate": round(win_rate, 3),
            "avg_win": round(avg_win, 3),
            "avg_loss": round(avg_loss, 3)
        }
    
    
    def calculate_profit(self, profits_detail, show_plot: bool = True):
        # 取得所有收益
        profits = [float(p["pnl"]) for p in profits_detail]
        
        # 計算總交易次數
        total_trades = len(profits)
        
        # 多單數量
        long_trades = len([p for p in profits_detail if p["position_type"] == "LONG"])
        
        # 空單數量
        short_trades = len([p for p in profits_detail if p["position_type"] == "SHORT"])
        
        # 多空比例
        long_short_ratio = long_trades / short_trades if short_trades > 0 else 0
        
        # 單日最大開單數量
        max_daily_trades = max([len([p for p in profits_detail if p["entry_time"].split(" ")[0] == d]) for d in set([p["entry_time"].split(" ")[0] for p in profits_detail])]) if profits_detail else 0
        
        # 計算總收益
        total_profit = sum(profits)
        
        # 計算勝率
        win_trades = len([p for p in profits if p > 0])
        loss_trades = len([p for p in profits if p < 0])
        win_rate = win_trades / total_trades * 100 if total_trades > 0 else 0
        
        # 計算平均收益
        avg_profit = total_profit / total_trades if total_trades > 0 else 0
        
        # 平均獲勝
        avg_win = sum([p for p in profits if p > 0]) / len([p for p in profits if p > 0]) if len([p for p in profits if p > 0]) > 0 else 0
        
        # 平均虧損
        avg_loss = sum([p for p in profits if p < 0]) / len([p for p in profits if p < 0]) if len([p for p in profits if p < 0]) > 0 else 0
        
        # 盈虧比
        profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
        
        # 最大單筆收益和最大單筆虧損
        if [p for p in profits if p > 0]:
            max_single_win = max([p for p in profits if p > 0])
        else:
            max_single_win = 0
        
        if [p for p in profits if p < 0]:
            max_single_loss = min([p for p in profits if p < 0])
        else:
            max_single_loss = 0
        
        # 計算最大連續回撤
        max_accumulated_drawdown = 0
        
        for i in range(len(profits)):
            for j in range(i+1, len(profits)):
                if sum(profits[i:j]) - profits[j] < max_accumulated_drawdown:
                    max_accumulated_drawdown = sum(profits[i:j]) - profits[j]
        
        
        # 累計增減
        cumsum = [0]
        for i in profits:
            cumsum.append(cumsum[-1] + i)
        
        if show_plot:
            self.plot_cumsum(cumsum)
            self.plot_all_profits(profits)
        
        return {
            "total_trades": total_trades,
            "long_trades": long_trades,
            "short_trades": short_trades,
            "long_short_ratio": round(long_short_ratio, 3),
            "max_daily_trades": max_daily_trades,
            "total_profit": round(total_profit, 3),
            "win_trades": win_trades,
            "loss_trades": loss_trades,
            "win_rate": round(win_rate, 3),
            "avg_profit": round(avg_profit, 3),
            "avg_win": round(avg_win, 3),
            "avg_loss": round(avg_loss, 3),
            "profit_loss_ratio": round(profit_loss_ratio, 3),
            "max_single_win": round(max_single_win, 3),
            "max_single_loss": round(max_single_loss, 3),
            "max_accumulated_drawdown": round(max_accumulated_drawdown, 3),
        }
    
    
    def calculate_profit_path(self, profit_record_path: str = None, profit_record_folder: str = "data/backtest_profit_record"):
        
        # 如果 profit_record_path 已經指定，則直接使用它
        # 否則則使用預設的 profit_record_folder
        # 命名規則： "{profit_record_folder}/profits_{i}.csv"
        # 故優先取 i 最大的檔案
        if not profit_record_path:
            profit_record_path = self._get_latest_profit_record_path(profit_record_folder)
            
        with open(profit_record_path, 'r') as f:
            profits_detail = list(csv.DictReader(f))
        
        return self.calculate_profit(profits_detail)
    
    
    def analyze_symbol(self, symbol: str, profit_record_path: str = None, profit_record_folder: str = "data/backtest_profit_record", show_plot: bool = True):
        """
        分析單一交易對的利潤情況
        :param symbol: 交易對名稱
        :param profit_record_path: 利潤記錄檔案路徑，如果為 None，則使用預設的資料夾
        :param profit_record_folder: 利潤記錄檔案所在的資料夾
        :return: 該交易對的利潤分析結果
        """
        
        if not profit_record_path:
            profit_record_path = self._get_latest_profit_record_path(profit_record_folder)
        
        with open(profit_record_path, 'r') as f:
            profits_detail = list(csv.DictReader(f))
        
        symbol_profits = [p for p in profits_detail if p["symbol"] == symbol]
        
        if not symbol_profits:
            return None
        
        return self.calculate_profit(symbol_profits, show_plot=show_plot)
    
    
    def analyze_symbols(self, profit_record_path: str = None, profit_record_folder: str = "data/backtest_profit_record", show_plot: bool = False):
        
        if not profit_record_path:
            profit_record_path = self._get_latest_profit_record_path(profit_record_folder)
            
        with open(profit_record_path, 'r') as f:
            profits_detail = list(csv.DictReader(f))
        
        symbols = set(p["symbol"] for p in profits_detail)
        analyzed_symbols = {}
        for symbol in symbols:
            analyzed_symbols[symbol] = self.calculate_profit(
                [p for p in profits_detail if p["symbol"] == symbol],
                show_plot=show_plot
            )
            print(f"Analyzed {symbol}: {analyzed_symbols[symbol]}")
        
        return analyzed_symbols
    
    
    # -------------------- Auxiliary Methods --------------------
    @staticmethod
    def _get_latest_profit_record_path(profit_record_folder):
        """
        獲取最新的利潤記錄檔案路徑
        :param profit_record_folder: 利潤記錄檔案所在的資料夾
        :return: 最新的利潤記錄檔案路徑，如果沒有找到符合條件的檔案則返回 None
        """
        
        # 確保資料夾存在
        if not os.path.exists(profit_record_folder):
            return None
        
        # 獲取所有符合命名規則的檔案
        pattern = os.path.join(profit_record_folder, "profits_*.csv")
        files = glob.glob(pattern)
        
        valid_files = [f for f in files if re.search(r"profits_(\d+)\.csv$", os.path.basename(f))]
        
        if not files:
            return None
        
        # 取得檔案名稱中的數字部分，並找到最大的數字
        latest_file = max(valid_files, key=lambda x: int(x.split('_')[-1].split('.')[0]))
        return latest_file if os.path.isfile(latest_file) else None
        
    
    