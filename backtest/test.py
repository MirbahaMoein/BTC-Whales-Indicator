import backtrader as bt

cerebro = bt.Cerebro()

cerebro.broker.setcash(10000)

cerebro.adddata()

print(cerebro.broker.getvalue())