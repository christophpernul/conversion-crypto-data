import os
import numpy as np
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
        filename_ledger = "kraken_ledgers.csv"
        filename_trades = "kraken_trades.csv"
        self.deposits_input = pd.read_csv(os.path.join(self.raw_path, filename_ledger))
        self.trades_input = pd.read_csv(os.path.join(self.raw_path, filename_trades))
    def convert_deposits(self):
        self.deposits_input.drop(["subtype", "aclass"], axis=1, inplace=True)
        # Drop first letter of shortcut of currency: X for crypto, Z for cash
        self.deposits_input["asset"] = self.deposits_input["asset"].apply(lambda shortcut: shortcut[1:])

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
        self.deposits["fee_currency"] = np.nan

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

        # Preprocessing of BUY entries
        buys = self.trades_input[self.trades_input["type"] == "buy"].copy()
        buys["currency_received"] = buys["pair"].apply(lambda pair: pair[:len(pair) // 2][1:])
        buys["currency_spent"] = buys["pair"].apply(lambda pair: pair[len(pair) // 2:][1:])
        buys.drop("pair", axis=1, inplace=True)
        buys = buys.rename(columns={"time": "date",
                                    "price": "conversion_rate_received_spent",
                                    "cost": "amount_spent",
                                    "fee": "fee",
                                    "vol": "amount_received"
                                    }
                           )

        # # Preprocessing of SELL entries
        sells = self.trades_input[self.trades_input["type"] == "sell"].copy()
        sells["currency_spent"] = sells["pair"].apply(lambda pair: pair[:len(pair) // 2][1:])
        sells["currency_received"] = sells["pair"].apply(lambda pair: pair[len(pair) // 2:][1:])
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

        # Combine BUY and SELL into single table
        self.trades = pd.concat([buys, sells], ignore_index=True)
        self.trades["date"] = pd.to_datetime(self.trades["date"], format='%Y-%m-%d %H:%M:%S')
        self.trades["date_string"] = self.trades["date"].dt.strftime('%Y-%m-%d')
        self.trades["fee_currency"] = self.trades["currency_spent"]

class Kucoin(Exchange):
    def __init__(self):
        Exchange.__init__(self)
        # Load ledger and trades data for kraken.com
        filename_ledger = "kucoin_deposits.csv"
        filename_trades = "kucoin_trades.csv"
        self.deposits_input = pd.read_csv(os.path.join(self.raw_path, filename_ledger))
        self.trades_input = pd.read_csv(os.path.join(self.raw_path, filename_trades))
    def convert_deposits(self):
        """
        TODO: Check in future if there is additional logic for withdrawals!
        :return:
        """
        self.deposits = self.deposits_input.rename(columns={"Time": "date",
                                                       "Coin": "currency_received",
                                                       "Amount": "amount_received"
                                                       }
                                              ).drop(["Type", "Remark"], axis=1)
        self.deposits["date"] = pd.to_datetime(self.deposits["date"], format='%Y-%m-%d %H:%M:%S')
        self.deposits["date_string"] = self.deposits["date"].dt.strftime('%Y-%m-%d')
        self.deposits["type"] = "deposit"


    def convert_trades(self):
        """
        BUYS: First currency in pair is the currency, that is received, second one is spent
        SELLS: First currency in pair is the currency, that is spent, second one is received
        Fee is paid in currency one receives with (example: buy BTC with EOS, fee is paid in BTC)
        currency_spent = conversion_rate * currency_gained
        :return:
        """
        self.trades_input = self.trades_input[["oid", "symbol", "dealPrice", "dealValue",
                                               "amount", "fee", "direction", "createdDate"]]
        self.trades_input["fee"] *= -1

        # Preprocessing of BUY entries
        buys = self.trades_input[self.trades_input["direction"] == "BUY"].copy()
        buys["currency_received"] = buys["symbol"].apply(lambda x: x.split("-")[0])
        buys["currency_spent"] = buys["symbol"].apply(lambda x: x.split("-")[1])
        buys = buys.rename(columns={"createdDate": "date",
                                    "dealValue": "amount_spent",
                                    "amount": "amount_received",
                                    "dealPrice": "conversion_rate_received_spent",
                                    "oid": "trxid",
                                    "direction": "type"
                                    }).drop("symbol", axis=1)
        buys["type"] = buys["type"].apply(lambda string: string.lower())
        buys["margin"] = 0.
        buys["ordertxid"] = np.nan
        buys["ordertype"] = "market"

        # # Preprocessing of SELL entries
        sells = self.trades_input[self.trades_input["direction"] == "SELL"].copy()
        sells["currency_received"] = sells["symbol"].apply(lambda x: x.split("-")[1])
        sells["currency_spent"] = sells["symbol"].apply(lambda x: x.split("-")[0])
        sells = sells.rename(columns={"createdDate": "date",
                                      "dealValue": "amount_received",
                                      "amount": "amount_spent",
                                      "dealPrice": "conversion_rate_received_spent",
                                      "oid": "trxid",
                                      "direction": "type"
                                      }).drop("symbol", axis=1)
        sells["conversion_rate_received_spent"] = 1. / sells["conversion_rate_received_spent"]
        sells["type"] = sells["type"].apply(lambda string: string.lower())
        sells["margin"] = 0.
        sells["ordertxid"] = np.nan
        sells["ordertype"] = "market"

        # Combine BUY and SELL into single table
        self.trades = pd.concat([buys, sells], ignore_index=True)
        self.trades["date"] = pd.to_datetime(self.trades["date"], format='%Y-%m-%d %H:%M:%S')
        self.trades["date_string"] = self.trades["date"].dt.strftime('%Y-%m-%d')
        self.trades["fee_currency"] = self.trades["currency_received"]
        self.trades["ordertxid"] = self.trades["ordertxid"].astype(str)



