from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import requests


REST_BASE = {
    "practice": "https://api-fxpractice.oanda.com",
    "live": "https://api-fxtrade.oanda.com",
}

STREAM_BASE = {
    "practice": "https://stream-fxpractice.oanda.com",
    "live": "https://stream-fxtrade.oanda.com",
}


@dataclass(frozen=True)
class OandaCredentials:
    token: str
    environment: str = "practice"
    account_id: str | None = None

    @classmethod
    def from_env(cls, env_path: Path | None = None) -> "OandaCredentials":
        load_dotenv(env_path)
        token = first_env("OANDA_API_TOKEN", "OANDA_API_KEY")
        environment = os.getenv("OANDA_ENVIRONMENT", "practice").strip().lower() or "practice"
        account_id = os.getenv("OANDA_ACCOUNT_ID", "").strip() or None
        if environment not in REST_BASE:
            raise SystemExit(f"Unsupported OANDA environment: {environment}")
        if not token:
            raise SystemExit("Missing OANDA_API_TOKEN / OANDA_API_KEY.")
        return cls(token=token, environment=environment, account_id=account_id)


def load_dotenv(env_path: Path | None = None) -> None:
    candidates: list[Path] = []
    if env_path is not None:
        candidates.append(env_path.expanduser().resolve())
    repo_candidate = Path(__file__).resolve().parent / ".env"
    cwd_candidate = Path.cwd() / ".env"
    candidates.extend([repo_candidate, cwd_candidate])
    for path in candidates:
        if not path.exists():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            os.environ.setdefault(key, value)
        break


def first_env(*keys: str) -> str:
    for key in keys:
        value = os.getenv(key, "").strip()
        if value:
            return value
        for scope in ("User", "Machine"):
            scoped = os.environ.get(key, "").strip()
            if scoped:
                return scoped
            try:
                import winreg

                root = winreg.HKEY_CURRENT_USER if scope == "User" else winreg.HKEY_LOCAL_MACHINE
                subkey = r"Environment" if scope == "User" else r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment"
                with winreg.OpenKey(root, subkey) as handle:
                    scoped, _ = winreg.QueryValueEx(handle, key)
                scoped = str(scoped).strip()
                if scoped:
                    return scoped
            except OSError:
                continue
    return ""


@dataclass(frozen=True)
class OandaInstrument:
    name: str
    display_precision: int
    pip_location: int
    trade_units_precision: int
    minimum_trade_size: float

    @property
    def pip_size(self) -> float:
        return 10.0 ** int(self.pip_location)


class OandaClient:
    def __init__(self, credentials: OandaCredentials, timeout: int = 60) -> None:
        self.credentials = credentials
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {credentials.token}",
                "Content-Type": "application/json",
            }
        )
        self.account_id = credentials.account_id or self.resolve_account_id()

    @property
    def rest_base(self) -> str:
        return REST_BASE[self.credentials.environment]

    @property
    def stream_base(self) -> str:
        return STREAM_BASE[self.credentials.environment]

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        stream: bool = False,
        retries: int = 5,
    ) -> Any:
        url = f"{self.rest_base}{path}"
        delay = 1.0
        for attempt in range(retries):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=payload,
                    timeout=self.timeout,
                    stream=stream,
                )
                if response.status_code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                    time.sleep(delay)
                    delay *= 2.0
                    continue
                response.raise_for_status()
                if stream:
                    return response
                if not response.text.strip():
                    return {}
                return response.json()
            except requests.RequestException:
                if attempt == retries - 1:
                    raise
                time.sleep(delay)
                delay *= 2.0
        raise RuntimeError("unreachable")

    def stream_request(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        retries: int = 5,
    ) -> requests.Response:
        url = f"{self.stream_base}{path}"
        delay = 1.0
        for attempt in range(retries):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout, stream=True)
                if response.status_code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                    time.sleep(delay)
                    delay *= 2.0
                    continue
                response.raise_for_status()
                return response
            except requests.RequestException:
                if attempt == retries - 1:
                    raise
                time.sleep(delay)
                delay *= 2.0
        raise RuntimeError("unreachable")

    def list_accounts(self) -> list[dict[str, Any]]:
        payload = self.request("GET", "/v3/accounts")
        return list(payload.get("accounts", []))

    def resolve_account_id(self) -> str:
        accounts = self.list_accounts()
        if not accounts:
            raise SystemExit("No OANDA accounts were returned for the supplied token.")
        account_ids = [str(account["id"]) for account in accounts]
        summaries: list[dict[str, Any]] = []
        for account_id in account_ids:
            payload = self.request("GET", f"/v3/accounts/{account_id}/summary")
            summaries.append(dict(payload.get("account", {})))

        if self.credentials.environment == "practice":
            preferred_keywords = ("trial", "demo", "practice", "paper")
            preferred = [
                summary
                for summary in summaries
                if any(keyword in str(summary.get("alias", "")).strip().lower() for keyword in preferred_keywords)
            ]
            if preferred:
                preferred.sort(key=lambda summary: (str(summary.get("alias", "")).lower(), str(summary.get("id", ""))))
                return str(preferred[0]["id"])

        summaries.sort(key=lambda summary: str(summary.get("id", "")))
        return str(summaries[0]["id"])

    def account_summary(self) -> dict[str, Any]:
        payload = self.request("GET", f"/v3/accounts/{self.account_id}/summary")
        return dict(payload.get("account", {}))

    def list_open_trades(self) -> list[dict[str, Any]]:
        payload = self.request("GET", f"/v3/accounts/{self.account_id}/openTrades")
        return list(payload.get("trades", []))

    def list_pending_orders(self) -> list[dict[str, Any]]:
        payload = self.request("GET", f"/v3/accounts/{self.account_id}/pendingOrders")
        return list(payload.get("orders", []))

    def transactions_since(self, last_transaction_id: str) -> dict[str, Any]:
        return self.request(
            "GET",
            f"/v3/accounts/{self.account_id}/transactions/sinceid",
            params={"id": str(last_transaction_id)},
        )

    def get_instruments(self, instruments: list[str] | None = None) -> dict[str, OandaInstrument]:
        params = {"instruments": ",".join(instruments)} if instruments else None
        payload = self.request("GET", f"/v3/accounts/{self.account_id}/instruments", params=params)
        out: dict[str, OandaInstrument] = {}
        for instrument in payload.get("instruments", []):
            item = OandaInstrument(
                name=str(instrument["name"]),
                display_precision=int(instrument["displayPrecision"]),
                pip_location=int(instrument["pipLocation"]),
                trade_units_precision=int(instrument.get("tradeUnitsPrecision", 0)),
                minimum_trade_size=float(instrument.get("minimumTradeSize", "1")),
            )
            out[item.name] = item
        return out

    def format_price(self, instrument: OandaInstrument, price: float) -> str:
        return f"{float(price):.{instrument.display_precision}f}"

    def format_units(self, instrument: OandaInstrument, units: float) -> str:
        precision = max(0, int(instrument.trade_units_precision))
        if precision == 0:
            return str(int(round(units)))
        return f"{float(units):.{precision}f}"

    def pricing_stream(self, instruments: list[str]) -> Iterator[dict[str, Any]]:
        response = self.stream_request(
            f"/v3/accounts/{self.account_id}/pricing/stream",
            params={"instruments": ",".join(instruments)},
        )
        for line in response.iter_lines():
            if not line:
                continue
            yield json.loads(line.decode("utf-8"))

    def place_market_order(
        self,
        *,
        instrument: OandaInstrument,
        units: float,
        stop_loss_price: float | None = None,
        client_id: str | None = None,
        tag: str | None = None,
    ) -> dict[str, Any]:
        order: dict[str, Any] = {
            "instrument": instrument.name,
            "units": self.format_units(instrument, units),
            "type": "MARKET",
            "timeInForce": "FOK",
            "positionFill": "DEFAULT",
        }
        if stop_loss_price is not None:
            order["stopLossOnFill"] = {
                "timeInForce": "GTC",
                "price": self.format_price(instrument, stop_loss_price),
            }
        if client_id or tag:
            order["clientExtensions"] = {
                "id": client_id or "",
                "tag": tag or "",
            }
        return self.request("POST", f"/v3/accounts/{self.account_id}/orders", payload={"order": order})

    def place_limit_order(
        self,
        *,
        instrument: OandaInstrument,
        units: float,
        price: float,
        client_id: str | None = None,
        tag: str | None = None,
        reduce_only: bool = False,
    ) -> dict[str, Any]:
        order: dict[str, Any] = {
            "instrument": instrument.name,
            "units": self.format_units(instrument, units),
            "type": "LIMIT",
            "timeInForce": "GTC",
            "positionFill": "REDUCE_ONLY" if reduce_only else "DEFAULT",
            "price": self.format_price(instrument, price),
        }
        if client_id or tag:
            order["clientExtensions"] = {
                "id": client_id or "",
                "tag": tag or "",
            }
        return self.request("POST", f"/v3/accounts/{self.account_id}/orders", payload={"order": order})

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        return self.request("PUT", f"/v3/accounts/{self.account_id}/orders/{order_id}/cancel")

    def close_trade(self, trade_id: str, units: str = "ALL") -> dict[str, Any]:
        return self.request(
            "PUT",
            f"/v3/accounts/{self.account_id}/trades/{trade_id}/close",
            payload={"units": units},
        )

    def update_trade_orders(
        self,
        trade_id: str,
        *,
        take_profit_price: float | None = None,
        stop_loss_price: float | None = None,
        trailing_stop_distance: float | None = None,
        instrument: OandaInstrument | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if take_profit_price is not None:
            if instrument is None:
                raise ValueError("instrument is required when setting take_profit_price")
            payload["takeProfit"] = {
                "timeInForce": "GTC",
                "price": self.format_price(instrument, take_profit_price),
            }
        if stop_loss_price is not None:
            if instrument is None:
                raise ValueError("instrument is required when setting stop_loss_price")
            payload["stopLoss"] = {
                "timeInForce": "GTC",
                "price": self.format_price(instrument, stop_loss_price),
            }
        if trailing_stop_distance is not None:
            payload["trailingStopLoss"] = {
                "distance": f"{float(trailing_stop_distance):.8f}",
                "timeInForce": "GTC",
            }
        return self.request("PUT", f"/v3/accounts/{self.account_id}/trades/{trade_id}/orders", payload=payload)
