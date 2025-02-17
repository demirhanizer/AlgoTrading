import pytest
import time
from unittest.mock import patch, MagicMock
from execute import get_latest_trade_signal, place_order, check_for_trade_signals


@pytest.fixture
def mock_mongo():
    with patch("execute.signals_collection") as mock_signals, patch("execute.orders_collection") as mock_orders:
        yield mock_signals, mock_orders


@pytest.fixture
def mock_binance():
    with patch("execute.exchange") as mock_exchange:
        yield mock_exchange


@pytest.fixture
def mock_logger():
    with patch("execute.logger") as mock_logger:
        yield mock_logger


def test_get_latest_trade_signal(mock_mongo, mock_logger):
    mock_signals, _ = mock_mongo

    mock_signals.find_one.return_value = {"signal": "BUY", "price": 50000, "timestamp": "2025-02-12T12:00:00"}

    latest_signal = get_latest_trade_signal()

    assert latest_signal is not None
    assert latest_signal["signal"] == "BUY"
    assert latest_signal["price"] == 50000
    mock_logger.info.assert_called()


def test_get_latest_trade_signal_no_data(mock_mongo, mock_logger):
    mock_signals, _ = mock_mongo

    mock_signals.find_one.return_value = None

    latest_signal = get_latest_trade_signal()

    assert latest_signal is None
    mock_logger.warning.assert_called()


def test_place_order_buy(mock_mongo, mock_binance, mock_logger):
    _, mock_orders = mock_mongo
    mock_exchange = mock_binance

    mock_exchange.create_market_buy_order.return_value = {"id": "order_123"}
    mock_exchange.fetch_ticker.return_value = {"last": 50000}

    order = place_order("BUY", 50000)

    assert order is not None
    mock_exchange.create_market_buy_order.assert_called_once_with("BTC/USDT", 0.0001)
    mock_orders.insert_one.assert_called()
    mock_logger.info.assert_called()


def test_place_order_sell(mock_mongo, mock_binance, mock_logger):
    _, mock_orders = mock_mongo
    mock_exchange = mock_binance

    mock_exchange.create_market_sell_order.return_value = {"id": "order_124"}
    mock_exchange.fetch_ticker.return_value = {"last": 50000}

    order = place_order("SELL", 50000)

    assert order is not None
    mock_exchange.create_market_sell_order.assert_called_once_with("BTC/USDT", 0.0001)
    mock_orders.insert_one.assert_called()
    mock_logger.info.assert_called()


def test_place_order_invalid_signal(mock_mongo, mock_binance, mock_logger):
    _, mock_orders = mock_mongo
    mock_exchange = mock_binance

    order = place_order("HOLD", 50000)  # Invalid signal

    assert order is None
    mock_logger.warning.assert_called()
    mock_exchange.create_market_buy_order.assert_not_called()
    mock_exchange.create_market_sell_order.assert_not_called()
    mock_orders.insert_one.assert_not_called()


def test_check_for_trade_signals(mock_mongo, mock_binance, mock_logger):
    mock_signals, mock_orders = mock_mongo
    mock_exchange = mock_binance

    mock_signals.find_one.return_value = {"signal": "BUY", "price": 50000, "timestamp": "2025-02-12T12:00:00"}
    mock_exchange.create_market_buy_order.return_value = {"id": "order_125"}

    check_for_trade_signals()

    mock_logger.info.assert_called()
    mock_exchange.create_market_buy_order.assert_called_once()
    mock_orders.insert_one.assert_called()


def test_check_for_trade_signals_no_signal(mock_mongo, mock_binance, mock_logger):
    mock_signals, mock_orders = mock_mongo
    mock_exchange = mock_binance

    mock_signals.find_one.return_value = None

    check_for_trade_signals()

    mock_logger.warning.assert_called_with("⚠️ No trade signals found.")
    mock_exchange.create_market_buy_order.assert_not_called()
    mock_exchange.create_market_sell_order.assert_not_called()
    mock_orders.insert_one.assert_not_called()
