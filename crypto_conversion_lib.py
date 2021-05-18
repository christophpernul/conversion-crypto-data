import os
import numpy as np
import pandas as pd



class Exchange():
    def __init__(self):
        self.base_path = "/home/chris/Dropbox/Finance/data/crypto"
        self.raw_path = os.path.join(self.base_path, "raw")
        self.export_path = os.path.join(self.base_path, "exported")
        self.deposits = None
        self.trades = None
        self.trade_history = None
    def compute_total_investment(self):
        return(sum(self.deposits))
    def combine_deposits_trades(self):
        self.trade_history = pd.concat([self.deposits, self.trades], ignore_index=True)
    def save_trade_history(self, filename):
        assert type(self.trade_history) != None, "Combine deposits and trades first!"
        self.trade_history.to_csv(os.path.join(self.export_path, filename), sep=",")

def drop_first_letter_currency(currency):
    if len(currency) == 4 and currency not in ["QTUM", "IOTA"]:
        # In case the currency is 4-digits long and is not QTUM drop the first X (no information)
        # Examples: XXRP -> XRP, XETH -> ETH
        # Exception for QTUM and IOTA is done for binance: there the dropping of the first letter is not needed
        currency = currency[1:]
    return(currency)

def get_left_part_of_currency_pair(pair):
    """In case of Kraken split the currency pair correctly"""
    if pair[:4] in ["QTUM", "IOTA"]:
        # In case the left pair is QTUM, do not split in the middle
        left = pair[:4]
    else:
        # Split the pair in the middle of the string
        left = pair[:len(pair) // 2]
        left = drop_first_letter_currency(left)
    return(left)

def get_right_part_of_currency_pair(pair):
    """In case of Kraken split the currency pair correctly"""
    if pair[:4] in ["QTUM", "IOTA"]:
        # In case the left pair is QTUM, do not split in the middle
        right = pair[4:]
    else:
        # Split the pair in the middle of the string
        right = pair[len(pair) // 2:]
        right = drop_first_letter_currency(right)
    return(right)

def combine_file_content(raw_path, file_list):
    for idx, fname in enumerate(file_list):
        if idx == 0:
            df_all = pd.read_csv(os.path.join(raw_path, fname))
        else:
            df = pd.read_csv(os.path.join(raw_path, fname))
            df_all = pd.concat([df_all, df], ignore_index=True)
    return(df_all)

class Kraken(Exchange):
    def __init__(self):
        Exchange.__init__(self)
        # Load ledger and trades data for kraken.com
        file_list = os.listdir(self.raw_path)
        file_list_deposits = list(filter(lambda fname: "kraken_ledgers_" in fname, file_list))
        file_list_trades = list(filter(lambda fname: "kraken_trades_" in fname, file_list))

        self.deposits_input = combine_file_content(self.raw_path, file_list_deposits)
        self.trades_input = combine_file_content(self.raw_path, file_list_trades)
    def convert_deposits(self):
        self.deposits = self.deposits_input[self.deposits_input["type"] != "trade"].dropna().copy()

        self.deposits.drop(["subtype", "aclass", "balance"], axis=1, inplace=True)
        self.deposits["asset"] = self.deposits["asset"].apply(drop_first_letter_currency)

        self.deposits = self.deposits.rename(columns={"time": "date",
                                                      "refid": "ordertxid",
                                                      "asset": "currency"
                                                      }
                                             )
        self.deposits["conversion_rate_received_spent"] = np.nan
        self.deposits["margin"] = np.nan
        self.deposits["ordertype"] = np.nan
        self.deposits["fee"] *= -1


    def convert_trades(self):
        """
        Fee is paid in currency one pays with (example: buy BTC with EUR, fee is paid in EUR)
        currency_spent = conversion_rate * currency_gained
        Be aware that prices are only up to 6 significant digits!
        :return:
        """
        # Prepare the ledger files to extract the fee from it to join to the trades table
        # The currency of the fee is the currency of the row in which the fee is stated (2 entries per trx)
        ledger = self.deposits_input[self.deposits_input["type"] == "trade"]\
                                    .drop(["balance", "aclass", "subtype"], axis=1).copy()

        # Each trade entry is connected to two entries in the ledger (one entry for the currency received/spent each)
        self.trades_input["ledgers"] = self.trades_input["ledgers"].apply(lambda x: x.split(","))
        self.trades_input = self.trades_input.explode("ledgers")
        self.trades_input = self.trades_input[["txid", "ordertxid", "pair",
                                               "time", "type", "ledgers",
                                               "price", "cost", "fee",
                                               "vol", "margin"
                                               ]]

        # Join the ledger entries to each trade
        self.trades = self.trades_input.merge(ledger,
                                              how="outer",
                                              left_on=["txid", "ledgers"],
                                              right_on=["refid", "txid"],
                                              suffixes=["", "_ledger"]
                                              )
        # A row corresponds to the currency, that was received in a trade, if the amount is positive only
        self.trades["received_flag"] = self.trades["amount"].apply(lambda amount: np.heaviside(amount, 0))
        self.trades = self.trades.drop(["txid", "ordertxid",
                                          "ledgers", "time_ledger",
                                          "vol", "pair",
                                          "fee", "cost"], axis=1)
        assert (self.trades.groupby("refid")["received_flag"].sum() == 1.).all() == True, \
                "Some trades have inconsistent trade amounts (plus and minus)!"


        self.trades = self.trades.drop("received_flag", axis=1).copy()
        self.trades = self.trades.rename(columns={"txid_ledger": "txid",
                                                  "refid": "ordertxid",
                                                  "time": "date",
                                                  "asset": "currency",
                                                  "fee_ledger": "fee",
                                                  "price": "conversion_rate_received_spent",
                                                  "type": "ordertype",
                                                  "type_ledger": "type"
                                                  }
                                         )
        self.trades["fee"] *= -1



class Kucoin(Exchange):
    def __init__(self):
        Exchange.__init__(self)
        # Load ledger and trades data for kraken.com
        file_list = os.listdir(self.raw_path)
        file_list_deposits = list(filter(lambda fname: "kucoin_deposits_" in fname, file_list))
        file_list_trades = list(filter(lambda fname: "kucoin_trades_" in fname, file_list))

        self.deposits_input = combine_file_content(self.raw_path, file_list_deposits)
        self.trades_input = combine_file_content(self.raw_path, file_list_trades)
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
                                    "oid": "txid",
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
                                      "oid": "txid",
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



class Binance(Exchange):
    def __init__(self):
        Exchange.__init__(self)
        # Load ledger and trades data for kraken.com
        (self.deposits_input, self.trades_input) = self.data_initialization()
    def data_initialization(self):
        filenames_deposits = list(filter(lambda fname: "binance_deposits" in fname, os.listdir(self.raw_path)))
        filenames_trades = list(filter(lambda fname: "binance_tradehistory" in fname, os.listdir(self.raw_path)))
        for counter, fname in enumerate(filenames_deposits):
            try:
                # Use xlrd library to load this type of excel xlsx path, but be aware that an old library 1.2.0 is used
                # since in newer versions this is not possible anymore.
                # Switch to openpyxl once this bug is solved: https://github.com/pandas-dev/pandas/issues/39001
                df = pd.read_excel(os.path.join(self.raw_path, fname))
                if counter == 0:
                    deposits_init = df
                else:
                    deposits_init = pd.concat([deposits_init, df], ignore_index=True, sort=False)
            except pd.errors.EmptyDataError:
                print(pd.errors.EmptyDataError, " for ", fname)

        for counter, fname in enumerate(filenames_trades):
            try:
                dft = pd.read_csv(os.path.join(self.raw_path, fname))
                if counter == 0:
                    trades_init = dft
                else:
                    trades_init = pd.concat([trades_init, dft], ignore_index=True, sort=False)
            except pd.errors.EmptyDataError:
                print(pd.errors.EmptyDataError, " for ", fname)

        return ((deposits_init, trades_init))

    def convert_deposits(self):
        # TODO: Check for withdrawals!
        self.deposits = self.deposits_input.rename(columns={"Date(UTC)": "date",
                                                                  "Coin": "currency_received",
                                                                  "Amount": "amount_received",
                                                                  "TransactionFee": "fee",
                                                                  "TXID": "txid",
                                                                  "PaymentID": "ordertxid"}
                                                       ).drop(["Status", "SourceAddress", "Address"], axis=1)
        self.deposits["type"] = "deposit"
        # self.deposits["balance"] = np.nan
        # self.deposits["currency_spent"] = np.nan
        # self.deposits["amount_spent"] = np.nan
        # self.deposits["fee_currency"] = np.nan
        self.deposits["date"] = pd.to_datetime(self.deposits["date"], format='%Y-%m-%d %H:%M:%S')
        self.deposits["date_string"] = self.deposits["date"].dt.strftime('%Y-%m-%d')
        self.deposits["fee"] = self.deposits["fee"].astype(float)
        self.deposits["ordertxid"] = self.deposits["ordertxid"].astype(str)

    def convert_trades(self):
        """
        BUYS: First currency in pair is the currency, that is received, second one is spent
        SELLS: First currency in pair is the currency, that is spent, second one is received
        Fee is paid in currency one receives with (example: buy BTC with EOS, fee is paid in BTC)
        currency_spent = conversion_rate * currency_gained
        :return:
        """
        buys = self.trades_input[self.trades_input["Side"] == "BUY"].copy()
        buys["currency_received"] = buys["Pair"].apply(get_left_part_of_currency_pair)
        buys["currency_spent"] = buys["Pair"].apply(get_right_part_of_currency_pair)
        buys = buys.rename(columns={"Date(UTC)": "date",
                                    "Side": "type",
                                    "Amount": "amount_spent",
                                    "Executed": "amount_received",
                                    "Fee": "fee",
                                    "Price": "conversion_rate_received_spent"
                                    }).drop(["Pair"], axis=1)
        buys["type"] = buys["type"].apply(lambda string: string.lower())
        buys["amount_received"] = buys["amount_received"].str.replace(",", "").str.replace("[a-zA-Z]+", "")
        buys["amount_spent"] = buys["amount_spent"].str.replace(",", "").str.replace("[a-zA-Z]+", "")
        buys["fee_currency"] = buys["fee"].str.extract("([a-zA-Z]+)")
        buys["fee"] = buys["fee"].str.replace(",", "").str.replace("[a-zA-Z]+", "")
        buys["margin"] = 0.
        buys["balance"] = np.nan
        buys["txid"] = np.nan
        buys["ordertxid"] = np.nan
        buys["ordertype"] = "market"


        sells = self.trades_input[self.trades_input["Side"] == "SELL"].copy()
        sells["currency_spent"] = sells["Pair"].apply(get_left_part_of_currency_pair)
        sells["currency_received"] = sells["Pair"].apply(get_right_part_of_currency_pair)
        sells = sells.rename(columns={"Date(UTC)": "date",
                                      "Side": "type",
                                      "Amount": "amount_received",
                                      "Executed": "amount_spent",
                                      "Fee": "fee",
                                      "Price": "conversion_rate_received_spent"
                                      }).drop(["Pair"], axis=1)
        sells["conversion_rate_received_spent"] = 1. / sells["conversion_rate_received_spent"]
        sells["type"] = sells["type"].apply(lambda string: string.lower())
        sells["amount_received"] = sells["amount_received"].str.replace(",", "").str.replace("[a-zA-Z]+", "")
        sells["amount_spent"] = sells["amount_spent"].str.replace(",", "").str.replace("[a-zA-Z]+", "")
        sells["fee_currency"] = sells["fee"].str.extract("([a-zA-Z]+)")
        sells["fee"] = sells["fee"].str.replace(",", "").str.replace("[a-zA-Z]+", "")
        sells["margin"] = 0.
        sells["balance"] = np.nan
        sells["txid"] = np.nan
        sells["ordertxid"] = np.nan
        sells["ordertype"] = "market"

        # Combine BUY and SELL into single table
        self.trades = pd.concat([buys, sells], ignore_index=True)
        self.trades["date"] = pd.to_datetime(self.trades["date"], format='%Y-%m-%d %H:%M:%S')
        self.trades["date_string"] = self.trades["date"].dt.strftime('%Y-%m-%d')
        self.trades["ordertxid"] = self.trades["ordertxid"].astype(str)
        self.trades["fee"] = self.trades["fee"].astype(float)
        self.trades["fee"] *= -1