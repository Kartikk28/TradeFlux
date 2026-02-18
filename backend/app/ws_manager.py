
from typing import Dict, Set
from fastapi import WebSocket

class WSManager:
    def __init__(self):
        self.active: Dict[str, Set[WebSocket]] = {}

    async def connect(self, symbol: str, websocket: WebSocket):
        await websocket.accept()
        self.active.setdefault(symbol, set()).add(websocket)

    def disconnect(self, symbol: str, websocket: WebSocket):
        if symbol in self.active and websocket in self.active[symbol]:
            self.active[symbol].remove(websocket)
            if not self.active[symbol]:
                del self.active[symbol]

    async def broadcast(self, symbol: str, message: dict):
        conns = self.active.get(symbol, set())
        to_remove = set()
        for ws in conns:
            try:
                await ws.send_json(message)
            except Exception:
                to_remove.add(ws)
        for ws in to_remove:
            self.disconnect(symbol, ws)
