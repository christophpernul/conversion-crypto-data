import os
import pandas as pd

class Exchange():
    def __init__(self):
        self.base_path = "/home/chris/Dropbox/Finance/data/crypto"
        self.raw_path = os.path.join(self.base_path, "raw")
        self.deposits = None
        self.trades = None
        self.trade_history = None
    def compute_total_investment(self):
        return(sum(self.deposits))
    def combine_deposits_trades(self):
        self.trade_history = pd.concat([self.deposits, self.trades], ignore_index=True)

class Kraken(Exchange):
    def __init__(self):
        Exchange.__init__(self)
        # Load ledger and trades data for kraken.com
        filename_kraken_ledger = "kraken_ledgers.csv"
        filename_kraken_trades = "kraken_trades.csv"
        self.deposits_input = pd.read_csv(os.path.join(self.raw_path, filename_kraken_ledger))
        self.trades_input = pd.read_csv(os.path.join(self.raw_path, filename_kraken_trades))
    def convert_deposits(self):
        self.deposits_input.drop(["subtype", "aclass"], axis=1, inplace=True)
        self.deposits = self.deposits_input[self.deposits_input["type"] == "deposit"].dropna().copy()
        self.deposits = self.deposits.rename(columns={"time": "date",
                                                        "asset": "currency_received",
                                                        "amount": "amount_received",
                                                        "fee": "fee"
                                                      }
                                             )
        withdrawals = self.deposits_input[self.deposits_input["type"] == "withdrawal"].dropna().copy()
        withdrawals = withdrawals.rename(columns={"time": "date",
                                                  "asset": "currency_spent",
                                                  "amount": "amount_spent",
                                                  "fee": "fee"
                                                  }
                                         )
        # Full outer join of deposits and withdrawals to get a single table in the same schema as trades table
        self.deposits = self.deposits.merge(withdrawals, how="outer")
        self.deposits.rename(columns={"refid": "ordertxid"}, inplace=True)
        self.deposits["fee"] *= -1
        self.deposits["date"] = pd.to_datetime(self.deposits["date"], format='%Y-%m-%d %H:%M:%S')
        self.deposits["date_string"] = self.deposits["date"].dt.strftime('%Y-%m-%d')

    def convert_trades(self):
        """
        BUYS: First currency in pair is the currency, that is received, second one is spent
        SELLS: First currency in pair is the currency, that is spent, second one is received
        Fee is paid in currency one pays with (example: buy BTC with EUR, fee is paid in EUR)
        currency_spent = conversion_rate * currency_gained
        Be aware that prices are only up to 6 significant digits!
        :return:
        """
        self.trades_input = self.trades_input[["txid", "ordertxid", "pair", "time", "type",
                                               "ordertype", "price", "cost", "fee", "vol", "margin"]]
        self.trades_input["fee"] *= -1

        buys = self.trades_input[self.trades_input["type"] == "buy"].copy()
        buys["currency_received"] = buys["pair"].apply(lambda pair: pair[:len(pair) // 2])
        buys["currency_spent"] = buys["pair"].apply(lambda pair: pair[len(pair) // 2:])
        buys.drop("pair", axis=1, inplace=True)
        buys = buys.rename(columns={"time": "date",
                                    "price": "conversion_rate_received_spent",
                                    "cost": "amount_spent",
                                    "fee": "fee",
                                    "vol": "amount_received"
                                    }
                           )

        sells = self.trades_input[self.trades_input["type"] == "sell"].copy()
        sells["currency_spent"] = sells["pair"].apply(lambda pair: pair[:len(pair) // 2])
        sells["currency_received"] = sells["pair"].apply(lambda pair: pair[len(pair) // 2:])
        sells.drop("pair", axis=1, inplace=True)
        sells = sells.rename(columns={"time": "date",
                                      "price": "conversion_rate_received_spent",
                                      "cost": "amount_received",
                                      "fee": "fee",
                                      "vol": "amount_spent"
                                      }
                             )
        ## To have same definition of the conversion rate as for the buys above
        sells["conversion_rate_received_spent"] = 1. / sells["conversion_rate_received_spent"]
        print(buys.count()[0])
        print(sells.count()[0])
        self.trades = pd.concat([buys, sells], ignore_index=True)
        self.trades["date"] = pd.to_datetime(self.trades["date"], format='%Y-%m-%d %H:%M:%S')
        self.trades["date_string"] = self.trades["date"].dt.strftime('%Y-%m-%d')




