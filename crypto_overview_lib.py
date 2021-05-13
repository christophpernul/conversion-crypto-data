import os
import pandas as pd

def combine_exchange_data(path="/home/chris/Dropbox/Finance/data/crypto/exported"):
    kraken = pd.read_csv(os.path.join(path, "kraken.csv"))
    binance = pd.read_csv(os.path.join(path, "binance.csv"))
    kucoin = pd.read_csv(os.path.join(path, "kucoin.csv"))

    kraken["exchange"] = "kraken"
    binance["exchange"] = "binance"
    kucoin["exchange"] = "kucoin"

    df = pd.concat([kraken, binance, kucoin], ignore_index=True, sort=False)
    df.drop(columns=["Unnamed: 0", "txid", "ordertxid", "margin", "ordertype", "balance"], inplace=True)
    df.loc[df["currency_received"] == "XBT", "currency_received"] = "BTC"
    df.loc[df["currency_spent"] == "XBT", "currency_spent"] = "BTC"
    df.loc[df["fee_currency"] == "XBT", "fee_currency"] = "BTC"
    return(df.copy())

def prepare_coin_overview_table(crypto_trades):
    """
    Prepares an aggregation of all trades and cross-exchange deposits/withdrawals.
    Aggregation is done per exchange, date and currency.
    The resulting table allows to calculate total amount of coins, balance on each exchange etc.
    TODO: 13.5.2021: Kraken: Euro wrong (2714.62)
    TODO: 13.5.2021: Binance: TRX wrong, should be 972 instead of 926
    TODO: 13.5.2021: Kucoin: ETC wrong, should be 0.09 instead of 0.3
    :param crypto_trades: table, with all crypto-transactions from all exchanges (output of combine_exchange_data)
    :return:
    """
    coin_gains = crypto_trades[["exchange", "currency_received", "amount_received", "date_string"]]
    coin_sells = crypto_trades[["exchange", "currency_spent", "amount_spent", "date_string"]]
    fees_paid = crypto_trades[crypto_trades["fee_currency"] != "EUR"][["exchange", "fee_currency", "fee", "date_string"]]

    coin_gains = coin_gains.rename(columns={"currency_received": "currency",
                                            "amount_received": "amount"})
    coin_sells = coin_sells.rename(columns={"currency_spent": "currency",
                                            "amount_spent": "amount"})
    coin_sells["amount"] *= -1
    fees_paid = fees_paid.rename(columns={"fee_currency": "currency",
                                          "fee": "amount"})
    coins = pd.concat([coin_gains, coin_sells, fees_paid], ignore_index=True, sort=False)

    coins_agg = coins.groupby(["exchange", "date_string", "currency"]).sum().reset_index().copy()

    return(coins_agg)