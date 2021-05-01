import crypto_conversion_lib as cl

Kraken = cl.Kraken()
print("--------- KRAKEN ---------------")
Kraken.convert_deposits()
print("Deposits = ", Kraken.deposits.count()[0])
Kraken.convert_trades()
print("Trades = ", Kraken.trades.count()[0])
Kraken.combine_deposits_trades()
print("Total = ", Kraken.trade_history.count()[0])

print("--------- KUCOIN ---------------")
Kucoin = cl.Kucoin()
Kucoin.convert_deposits()
print("Deposits = ", Kucoin.deposits.count()[0])
Kucoin.convert_trades()
print("Trades = ", Kucoin.trades.count()[0])
Kucoin.combine_deposits_trades()
print("Total = ", Kucoin.trade_history.count()[0])

