
from collections import defaultdict, deque
from datetime import datetime
from typing import Dict, Deque, Tuple
import numpy as np

# Rolling EMA and VWAP per symbol
class RollingAnalytics:
    def __init__(self, ema_alpha: float = 0.2, vwap_window: int = 200):
        self.ema_alpha = ema_alpha
        self.vwap_window = vwap_window
        self._ema: Dict[str, float] = {}
        self._vwap_q: Dict[str, Deque[Tuple[float, int]]] = defaultdict(lambda: deque(maxlen=vwap_window))

    def update(self, symbol: str, price: float, volume: int) -> tuple[float | None, float | None]:
        # EMA
        prev = self._ema.get(symbol)
        ema = price if prev is None else (self.ema_alpha * price + (1 - self.ema_alpha) * prev)
        self._ema[symbol] = ema

        # VWAP rolling
        q = self._vwap_q[symbol]
        q.append((price, volume))
        if not q:
            vwap = None
        else:
            pv = sum(p * v for p, v in q)
            vv = sum(v for _, v in q)
            vwap = pv / vv if vv > 0 else None
        return ema, vwap

    def get_metrics(self, symbol: str):
        return {
            'ema': self._ema.get(symbol),
            'vwap': (lambda q=self._vwap_q[symbol]: (sum(p * v for p, v in q) / sum(v for _, v in q) if q and sum(v for _, v in q) > 0 else None))()
        }
