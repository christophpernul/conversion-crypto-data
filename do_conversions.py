import crypto_conversion_lib as cl

s = cl.Kraken()
s.convert_deposits()
# print(s.deposits.groupby(["currency_spent"]).sum())
# print(s.deposits.groupby(["currency_received"]).sum())
print("Deposits = ", s.deposits.count()[0])

s.convert_trades()
print("Trades = ", s.trades.count()[0])
s.combine_deposits_trades()

print("Total = ", s.trade_history.count()[0])
print(s.trade_history.iloc[0])