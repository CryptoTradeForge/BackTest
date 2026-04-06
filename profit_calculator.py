import os
import csv
import glob
import re
import math
from statistics import mean, stdev


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
        
        # 計算穩定度指標
        stability_metrics = self.calculate_stability_metrics(profits)

        base_result = {
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

        # 合併穩定度指標
        base_result.update(stability_metrics)
        return base_result
    
    
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
    

    def calculate_stability_metrics(self, profits):
        """
        計算所有穩定度指標
        :param profits: 交易利潤列表 (PnL 數值列表)
        :return: 包含所有穩定度指標的字典
        """
        if not profits or len(profits) < 2:
            return {
                "sharpe_ratio": float('nan'),
                "sortino_ratio": float('nan'),
                "calmar_ratio": float('nan'),
                "omega_ratio": float('nan'),
                "ulcer_index": float('nan'),
                "k_ratio": float('nan'),
                "rolling_sharpe_min": float('nan'),
                "rolling_sharpe_max": float('nan'),
                "rolling_sharpe_mean": float('nan'),
                "hurst_exponent": float('nan'),
            }

        metrics = {}

        # 1. Sharpe Ratio
        metrics["sharpe_ratio"] = round(self._calculate_sharpe_ratio(profits), 4)

        # 2. Sortino Ratio
        metrics["sortino_ratio"] = round(self._calculate_sortino_ratio(profits), 4)

        # 3. Calmar Ratio
        metrics["calmar_ratio"] = round(self._calculate_calmar_ratio(profits), 4)

        # 4. Omega Ratio
        metrics["omega_ratio"] = round(self._calculate_omega_ratio(profits), 4)

        # 5. Ulcer Index
        metrics["ulcer_index"] = round(self._calculate_ulcer_index(profits), 4)

        # 6. K-Ratio
        metrics["k_ratio"] = round(self._calculate_k_ratio(profits), 4)

        # 7. Rolling Sharpe (20-trade window)
        rolling_sharpes = self._calculate_rolling_sharpe(profits, window=20)
        metrics["rolling_sharpe_min"] = round(min(rolling_sharpes), 4) if rolling_sharpes else float('nan')
        metrics["rolling_sharpe_max"] = round(max(rolling_sharpes), 4) if rolling_sharpes else float('nan')
        metrics["rolling_sharpe_mean"] = round(mean(rolling_sharpes), 4) if rolling_sharpes else float('nan')

        # 8. Hurst Exponent
        metrics["hurst_exponent"] = round(self._calculate_hurst_exponent(profits), 4)

        return metrics


    def _calculate_sharpe_ratio(self, profits):
        """
        計算 Sharpe Ratio
        公式: (mean_return - risk_free_rate) / std_return * sqrt(252)
        risk_free_rate = 0，假設有 252 個交易日
        """
        if len(profits) < 2:
            return float('nan')

        try:
            mean_return = mean(profits)
            std_return = stdev(profits)

            if std_return == 0:
                return float('nan')

            # 年化因子：假設交易頻率相當於每日交易，所以用 252
            sharpe = (mean_return / std_return) * math.sqrt(252)
            return sharpe
        except:
            return float('nan')


    def _calculate_sortino_ratio(self, profits):
        """
        計算 Sortino Ratio
        公式: (mean_return - risk_free_rate) / downside_std_return * sqrt(252)
        只考慮負收益（下行波動）
        """
        if len(profits) < 2:
            return float('nan')

        try:
            mean_return = mean(profits)

            # 計算下行波動率（只看負收益）
            downside_returns = [p for p in profits if p < 0]
            if not downside_returns:
                # 沒有虧損交易，Sortino = Sharpe
                return self._calculate_sharpe_ratio(profits)

            downside_std = stdev(downside_returns) if len(downside_returns) > 1 else 0

            if downside_std == 0:
                return float('nan')

            sortino = (mean_return / downside_std) * math.sqrt(252)
            return sortino
        except:
            return float('nan')


    def _calculate_calmar_ratio(self, profits):
        """
        計算 Calmar Ratio
        公式: annualized_return / max_drawdown
        annualized_return = sum(profits) * 252 / len(profits)
        """
        if len(profits) < 2:
            return float('nan')

        try:
            # 計算年化收益率
            total_return = sum(profits)
            annualized_return = (total_return / len(profits)) * 252

            # 計算最大回撤（百分比）
            cumsum = [0]
            for p in profits:
                cumsum.append(cumsum[-1] + p)

            # 記錄最高點
            peak = cumsum[0]
            max_drawdown = 0
            for value in cumsum[1:]:
                if value > peak:
                    peak = value
                drawdown = peak - value
                if drawdown > max_drawdown:
                    max_drawdown = drawdown

            if max_drawdown == 0:
                return float('nan')

            calmar = annualized_return / max_drawdown
            return calmar
        except:
            return float('nan')


    def _calculate_omega_ratio(self, profits, threshold=0):
        """
        計算 Omega Ratio
        公式: sum(max(r - threshold, 0)) / sum(max(threshold - r, 0))
        衡量超過門檻值的上漲 vs 低於門檻值的下跌比例
        """
        if not profits:
            return float('nan')

        try:
            gains = sum(max(p - threshold, 0) for p in profits)
            losses = sum(max(threshold - p, 0) for p in profits)

            if losses == 0:
                return float('nan') if gains == 0 else float('inf')

            omega = gains / losses
            return omega
        except:
            return float('nan')


    def _calculate_ulcer_index(self, profits):
        """
        計算 Ulcer Index
        公式: sqrt(mean(drawdown_pct^2))
        衡量回撤的持續性和嚴重度
        """
        if len(profits) < 2:
            return float('nan')

        try:
            # 累計權益曲線
            cumsum = [0]
            for p in profits:
                cumsum.append(cumsum[-1] + p)

            # 計算每個點的回撤百分比
            drawdown_squared_list = []
            peak = cumsum[0]

            for value in cumsum[1:]:
                if value > peak:
                    peak = value

                if peak != 0:
                    drawdown_pct = (peak - value) / peak
                    drawdown_squared_list.append(drawdown_pct ** 2)

            if not drawdown_squared_list:
                return 0.0

            ulcer_index = math.sqrt(mean(drawdown_squared_list))
            return ulcer_index
        except:
            return float('nan')


    def _calculate_k_ratio(self, profits):
        """
        計算 K-Ratio
        公式: 股權曲線線性回歸斜率 / 斜率的標準誤
        衡量策略是否有穩定的線性上升趨勢
        """
        if len(profits) < 3:
            return float('nan')

        try:
            # 累計權益曲線
            cumsum = [0]
            for p in profits:
                cumsum.append(cumsum[-1] + p)

            # 線性回歸：y = mx + b
            n = len(cumsum)
            x = list(range(n))
            y = cumsum

            mean_x = mean(x)
            mean_y = mean(y)

            # 斜率計算
            numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
            denominator = sum((x[i] - mean_x) ** 2 for i in range(n))

            if denominator == 0:
                return float('nan')

            slope = numerator / denominator

            # 計算標準誤
            residuals = [y[i] - (slope * x[i] + (mean_y - slope * mean_x)) for i in range(n)]
            residual_std = stdev(residuals) if len(residuals) > 1 else 0

            if residual_std == 0:
                return float('nan')

            # K-Ratio = 斜率 / (斜率的標準誤)
            # 需要計算斜率的標準誤
            mse = mean(r ** 2 for r in residuals)
            slope_se = math.sqrt(mse / denominator) if denominator > 0 else 0

            if slope_se == 0:
                return float('nan')

            k_ratio = slope / slope_se
            return k_ratio
        except:
            return float('nan')


    def _calculate_rolling_sharpe(self, profits, window=20):
        """
        計算滾動 Sharpe Ratio
        :param profits: 交易利潤列表
        :param window: 滾動窗口大小（默認 20 筆交易）
        :return: 所有滾動窗口的 Sharpe Ratio 列表
        """
        if len(profits) < window:
            return []

        rolling_sharpes = []

        for i in range(len(profits) - window + 1):
            window_profits = profits[i:i + window]
            sharpe = self._calculate_sharpe_ratio(window_profits)
            if not math.isnan(sharpe):
                rolling_sharpes.append(sharpe)

        return rolling_sharpes


    def _calculate_hurst_exponent(self, profits, lags=None):
        """
        計算 Hurst Exponent (R/S 分析法)
        公式: log(R/S) = H * log(n)
        H > 0.5: 趨勢持續性（正相關）
        H = 0.5: 隨機遊走
        H < 0.5: 均值回歸（負相關）
        """
        if len(profits) < 10:
            return float('nan')

        try:
            if lags is None:
                # 自動選擇 lag 範圍
                lags = [int(len(profits) / (i + 2)) for i in range(5)]
                lags = [lag for lag in lags if lag > 0]

            tau = []

            for lag in lags:
                if lag >= len(profits):
                    continue

                # 計算累積回報率
                cumsum = [0]
                for p in profits[:len(profits) - (len(profits) % lag)]:
                    cumsum.append(cumsum[-1] + p)

                # 將累積回報率分成 lag 個區間
                mean_profit = mean(profits[:len(profits) - (len(profits) % lag)])

                # 計算 Range (R)
                ranges = []
                for i in range(0, len(cumsum) - lag, lag):
                    chunk = cumsum[i:i + lag + 1]
                    mean_chunk = mean(chunk)

                    # 計算相對於均值的偏差
                    y = [chunk[j] - mean_chunk for j in range(len(chunk))]

                    # 計算 Rescaled Range
                    w = [sum(y[:j + 1]) for j in range(len(y))]

                    if w:
                        R = max(w) - min(w)

                        # 計算標準差 (S)
                        S = stdev(y) if len(y) > 1 else 0

                        if S > 0:
                            ranges.append(R / S)

                if ranges:
                    tau.append(mean(ranges))

            if len(tau) < 2:
                return float('nan')

            # 線性回歸：log(tau) = H * log(lag)
            log_tau = [math.log(t) if t > 0 else 0 for t in tau]
            log_lags = [math.log(lag) for lag in lags[:len(tau)]]

            mean_log_lag = mean(log_lags)
            mean_log_tau = mean(log_tau)

            numerator = sum((log_lags[i] - mean_log_lag) * (log_tau[i] - mean_log_tau)
                          for i in range(len(log_lags)))
            denominator = sum((log_lags[i] - mean_log_lag) ** 2 for i in range(len(log_lags)))

            if denominator == 0:
                return float('nan')

            hurst = numerator / denominator
            return hurst
        except:
            return float('nan')


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
        
    
    