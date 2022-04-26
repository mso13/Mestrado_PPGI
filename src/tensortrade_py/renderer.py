import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tensortrade.env.generic import Renderer

class PositionChangeChart(Renderer):
    """The Renderer renders a view of the environment and interactions."""

    def __init__(self, color: str = "orange"):
        self.color = "orange"

    def render(self, env, **kwargs):
        # The Observer generates the next observation for the agent.
        history = pd.DataFrame(env.observer.renderer_history)

        actions = list(history.action)
        p = list(history.price)

        buy = {}
        sell = {}

        for i in range(len(actions) - 1):
            a1 = actions[i]
            a2 = actions[i + 1]

            if a1 != a2:
                if a1 == 0 and a2 == 1:
                    buy[i] = p[i]
                else:
                    sell[i] = p[i]
        
        buy = pd.Series(buy)
        sell = pd.Series(sell)

        fig, axs = plt.subplots(1, 2, figsize=(15, 5))

        fig.suptitle("Performance")

        axs[0].plot(np.arange(len(p)), p, label="price", color=self.color)
        axs[0].scatter(buy.index, buy.values, marker="v", color="red") # BUY
        axs[0].scatter(sell.index, sell.values, marker="^", color="green") # SELL
        axs[0].set_title("Trading Chart")
        axs[0].legend(['Price', 'Buys', 'Sells'])

        performance_df = pd.DataFrame().from_dict(env.action_scheme.portfolio.performance, orient='index')
        performance_df.plot(ax=axs[1])
        axs[1].set_title("Net Worth")

        plt.savefig('results.png')

        plt.show();