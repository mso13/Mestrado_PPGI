import sys
import ray
import numpy as np
import pandas as pd
import yfinance as yf
import pandas_ta as ta

from ray import tune
from ray.tune.registry import register_env

import tensortrade.env.default as default

from tensortrade.feed.core import DataFeed, Stream
from tensortrade.oms.exchanges import Exchange, ExchangeOptions
from tensortrade.oms.services.execution.simulated import execute_order
from tensortrade.oms.wallets import Wallet, Portfolio
from tensortrade.env.default.rewards import TensorTradeRewardScheme
from tensortrade.feed.core import Stream, DataFeed

from gym.spaces import Discrete
from tensortrade.env.default.actions import TensorTradeActionScheme
from tensortrade.env.generic import ActionScheme, TradingEnv
from tensortrade.core import Clock
from tensortrade.oms.instruments import ExchangePair, Instrument
from tensortrade.oms.wallets import Portfolio
from tensortrade.oms.orders import (
    Order,
    proportion_order,
    TradeSide,
    TradeType
)

import matplotlib.pyplot as plt

from tensortrade.env.generic import Renderer

import ray.rllib.agents.ppo as ppo

sys.path.append(".")

from actions import BSH
from reward import PBR
from renderer import PositionChangeChart


def generate_train_test_datasets(ticker):
    """Get Yahoo! Finance Data for Train/Test Splits."""

    TRAIN_START_DATE = '2021-01-01'
    TRAIN_END_DATE = '2021-09-30'
    EVAL_START_DATE = '2021-10-01'
    EVAL_END_DATE = '2022-01-21'
    yf_ticker = yf.Ticker(ticker=ticker)

    df_training = yf_ticker.history(start=TRAIN_START_DATE, end=TRAIN_END_DATE, interval='60m')
    df_training.drop(['Dividends', 'Stock Splits'], axis=1, inplace=True)
    df_training["Volume"] = df_training["Volume"].fillna(0).astype(int)
    df_training.ta.log_return(append=True, length=16)
    df_training.ta.rsi(append=True, length=14)
    df_training.ta.macd(append=True, fast=12, slow=26)
    df_training.dropna().to_csv('training.csv', index=True)

    df_evaluation = yf_ticker.history(start=EVAL_START_DATE, end=EVAL_END_DATE, interval='60m')
    df_evaluation.drop(['Dividends', 'Stock Splits'], axis=1, inplace=True)
    df_evaluation["Volume"] = df_evaluation["Volume"].fillna(0).astype(int)
    df_evaluation.ta.log_return(append=True, length=16)
    df_evaluation.ta.rsi(append=True, length=14)
    df_evaluation.ta.macd(append=True, fast=12, slow=26)
    df_evaluation.dropna().to_csv('evaluation.csv', index=True)

    return df_training, df_evaluation


def create_train_env(config):
    """Creates Trading Environment. """

    ticker = 'PETR4'

    # PRICES
    y = yf.download(f'{ticker}.SA', start='2021-01-01', end='2021-12-31')['Adj Close'].dropna().values
    price = Stream.source(y, dtype="float").rename("BRL-TICKER")

    bitfinex = Exchange("bitfinex",
                        service=execute_order)(price)

    # Portfolio
    cash = Wallet(bitfinex, 100000 * BRL)  # Money
    asset = Wallet(bitfinex, 0 * TICKER)  # Stocks

    portfolio = Portfolio(BRL, [cash, asset])

    # Data
    feed = DataFeed([
        price,
        price.rolling(window=10).mean().rename("fast"),
        price.rolling(window=50).mean().rename("medium"),
        price.rolling(window=100).mean().rename("slow"),
        price.log().diff().fillna(0).rename("lr")
    ])

    # Reward
    reward_scheme = PBR(price=price)

    # Actions
    action_scheme = BSH(
        cash=cash,
        asset=asset
    ).attach(reward_scheme)

    # Visualization
    renderer_feed = DataFeed([
        Stream.source(y, dtype="float").rename("price"),
        Stream.sensor(action_scheme, lambda s: s.action, dtype="float").rename("action")
    ])

    # Environment
    environment = default.create(
        feed=feed,
        portfolio=portfolio,
        action_scheme=action_scheme,
        reward_scheme=reward_scheme,
        renderer_feed=renderer_feed,
        renderer=PositionChangeChart(),
        window_size=config["window_size"],
        max_allowed_loss=0.6
    )

    return environment


def create_eval_env(config):
    y = config["y"]

    price = Stream.source(y, dtype="float").rename("BRL-TICKER")

    bitfinex = Exchange("bitfinex", service=execute_order)(price)

    cash = Wallet(bitfinex, 100000 * BRL)
    asset = Wallet(bitfinex, 0 * TICKER)

    portfolio = Portfolio(BRL, [cash, asset])

    feed = DataFeed([
        price,
        price.rolling(window=10).mean().rename("fast"),
        price.rolling(window=50).mean().rename("medium"),
        price.rolling(window=100).mean().rename("slow"),
        price.log().diff().fillna(0).rename("lr")
    ])

    reward_scheme = PBR(price=price)

    action_scheme = BSH(
        cash=cash,
        asset=asset
    ).attach(reward_scheme)

    renderer_feed = DataFeed([
        Stream.source(y, dtype="float").rename("price"),
        Stream.sensor(action_scheme, lambda s: s.action, dtype="float").rename("action")
    ])

    environment = default.create(
        feed=feed,
        portfolio=portfolio,
        action_scheme=action_scheme,
        reward_scheme=reward_scheme,
        renderer_feed=renderer_feed,
        renderer=PositionChangeChart(),
        window_size=config["window_size"],
        max_allowed_loss=0.6
    )
    return environment, portfolio


if __name__ == '__main__':

    # Instruments in Portfolio
    BRL = Instrument("BRL", 2, "Brazilian Currency")
    TICKER = Instrument("TICKER", 2, "Ticker")

    # Register Env
    register_env("TradingEnv", create_train_env)

    # Window Size
    window_size = 20

    #####################################################################################################

    # Run PPO Algorithm
    analysis = tune.run(
        "PPO",
        stop={
            "episode_reward_mean": 2.5
        },
        config={
            "env": "TradingEnv",
            "env_config": {
                "window_size": window_size
            },
            "log_level": "DEBUG",
            "framework": "tf2",
            "ignore_worker_failures": True,
            "num_workers": 1,
            "num_gpus": 1,
            "clip_rewards": True,
            "lr": 8e-6,
            "lr_schedule": [
                [0, 1e-1],
                [int(1e2), 1e-2],
                [int(1e3), 1e-3],
                [int(1e4), 1e-4],
                [int(1e5), 1e-5],
                [int(1e6), 1e-6],
                [int(1e7), 1e-7]
            ],
            "gamma": 0,
            "observation_filter": "MeanStdFilter",
            "lambda": 0.72,
            "vf_loss_coeff": 0.5,
            "entropy_coeff": 0.01
        },
        checkpoint_at_end=True
    )

    #####################################################################################################

    # Load Trained PPO Agent

    # Get checkpoint
    checkpoints = analysis.get_trial_checkpoints_paths(
        trial=analysis.get_best_trial("episode_reward_mean", mode="max"),
        metric="episode_reward_mean"
    )
    checkpoint_path = checkpoints[0][0]

    # Restore agent
    agent = ppo.PPOTrainer(
        env="TradingEnv",
        config={
            "env_config": {
                "window_size": window_size  # We want to look at the last x samples (days)
            },
            "framework": "tf2",
            "log_level": "DEBUG",
            "ignore_worker_failures": True,
            "num_workers": 1,
            "num_gpus": 1,
            "clip_rewards": True,
            "lr": 8e-6,
            "lr_schedule": [
                [0, 1e-1],
                [int(1e2), 1e-2],
                [int(1e3), 1e-3],
                [int(1e4), 1e-4],
                [int(1e5), 1e-5],
                [int(1e6), 1e-6],
                [int(1e7), 1e-7]
            ],
            "gamma": 0,
            "observation_filter": "MeanStdFilter",
            "lambda": 0.72,
            "vf_loss_coeff": 0.5,
            "entropy_coeff": 0.01
        }
    )

    agent.restore(checkpoint_path)

    # Access Trained PPO Agents Policy

    # Instantiate the environment
    env = create_train_env({
        "window_size": window_size
    })

    # Run until episode ends
    episode_reward = 0
    done = False
    obs = env.reset()

    while not done:
        action = agent.compute_single_action(obs)
        obs, reward, done, info = env.step(action)
        episode_reward += reward

    env.render()

    #####################################################################################################

    # Validation Set (New Data)

    env, portfolio = create_eval_env({
        "window_size": window_size,
        "y": yf.download(f'PETR4.SA', start='2022-01-01', end='2022-04-01')['Adj Close'].dropna().values
    })

    # Run until episode ends
    episode_reward = 0
    done = False
    obs = env.reset()

    while not done:
        action = agent.compute_single_action(obs)
        obs, reward, done, info = env.step(action)
        episode_reward += reward

    env.render()

    #####################################################################################################

    print (portfolio.ledger.as_frame().head(15))

    df = pd.DataFrame(portfolio.performance)

    df.loc["net_worth"].plot()