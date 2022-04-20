from gym.spaces import Discrete
from tensortrade.env.default.actions import TensorTradeActionScheme
from tensortrade.env.generic import ActionScheme, TradingEnv
from tensortrade.core import Clock
from tensortrade.oms.instruments import ExchangePair
from tensortrade.oms.wallets import Portfolio
from tensortrade.oms.orders import (
    Order,
    proportion_order,
    TradeSide,
    TradeType
)
from decimal import Decimal


class BSH(TensorTradeActionScheme):
    """The ActionScheme interprets and applies the agent’s actions to the environment."""

    registered_name = "bsh"

    def __init__(self,
                 cash: 'Wallet',
                 asset: 'Wallet'):
        super().__init__()
        self.cash = cash
        self.asset = asset

        self.listeners = []
        self.action = 0

    @property
    def action_space(self):
        return Discrete(2)

    def attach(self, listener):
        self.listeners += [listener]
        return self

    def get_orders(self,
                   action: int,
                   portfolio: 'Portfolio'):
        order = None

        if abs(action - self.action) > 0:
            src = self.cash if self.action == 0 else self.asset
            tgt = self.asset if self.action == 0 else self.cash

            if src == self.cash:
                # Calculates proportional order size (n lots of 100 shares)
                lot_size = 100.00
                current_price = float(portfolio.exchange_pairs[0].price)
                source_balance = src.balance.as_float()

                qtd_assets = source_balance / (lot_size * current_price)

                num_shares = int(qtd_assets - (qtd_assets % 10)) * lot_size

                proportional_lot_size = (num_shares * current_price) / source_balance
            else:
                proportional_lot_size = 1.0

            print('--' * 50)
            if src == self.cash:
                print ('CASH TO ASSET')
                print('Source Balance: ', src.balance.as_float())
                print('Target Balance: ', tgt.balance.as_float())
                print('Proportional Lot Size', proportional_lot_size)
                print('Current Price: ', float(portfolio.exchange_pairs[0].price))
                print('# Shares: ', num_shares)
                print('Current Price x # Shares: ', num_shares * float(portfolio.exchange_pairs[0].price))
            else:
                print ('ASSET TO CASH')
                print('Source Balance: ', src.balance.as_float())
                print('Target Balance: ', tgt.balance.as_float())
                print('Proportional Lot Size', proportional_lot_size)

            print('--' * 50)

            order = proportion_order(portfolio, src, tgt, proportional_lot_size)

            self.action = action

        for listener in self.listeners:
            listener.on_action(action)

        return [order]

    def reset(self):
        super().reset()
        self.action = 0