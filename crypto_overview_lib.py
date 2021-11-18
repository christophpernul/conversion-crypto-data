import os
import pandas as pd

def combine_exchange_data(path="/home/chris/Dropbox/Finance/data/crypto/exported"):
    kraken = pd.read_csv(os.path.join(path, "kraken_orders.csv"))
    binance = pd.read_csv(os.path.join(path, "binance_orders.csv"))
    kucoin = pd.read_csv(os.path.join(path, "kucoin_orders.csv"))

    kraken["exchange"] = "kraken"
    binance["exchange"] = "binance"
    kucoin["exchange"] = "kucoin"

    df = pd.concat([kraken, binance, kucoin], ignore_index=True, sort=False)
    df.drop(columns=["margin", "ordertype"], inplace=True)

    return(df.copy())

def prepare_coin_overview_table(crypto_trades):
    """
    Prepares an aggregation of all trades and cross-exchange deposits/withdrawals.
    Aggregation is done per exchange, date and currency.
    The resulting table allows to calculate total amount of coins, balance on each exchange etc.
    TODO: 13.5.2021: Binance: TRX wrong, should be 972 instead of 926
    TODO: 13.5.2021: Kucoin: ETC wrong, should be 0.09 instead of 0.37
    :param crypto_trades: table, with all crypto-transactions from all exchanges (output of combine_exchange_data)
    :return:
    """
    ## Aggregate all trades and paid fees
    coins = crypto_trades.groupby(["exchange", "date_string", "currency"])[["amount", "fee"]].sum()
    ## Sum amount of coins after all trades and paid fees
    overview = coins.stack().reset_index().groupby(["exchange", "date_string", "currency"]).sum().sort_index()
    overview = overview.reset_index().rename(columns={0: "amount"})

    return(overview)

export_path = "/home/chris/Dropbox/Finance/data/crypto/exported"
filename_output = "crypto_orders_longlist.csv"
filename_orders = "crypto_orders.csv"

df = combine_exchange_data(export_path)
df.to_csv(os.path.join(export_path, filename_orders), index=False)

out = prepare_coin_overview_table(df)

out.to_csv(os.path.join(export_path, filename_output), index=False)