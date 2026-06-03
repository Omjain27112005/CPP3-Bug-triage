import asyncio
import json
import structlog
from fastapi import WebSocket
from orchestrator.redis_client import get_redis, get_stored_panels, get_cached_case_result

log = structlog.get_logger()


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}

    async def connect(self, case_id: str, websocket: WebSocket) -> None:
        # Do NOT call websocket.accept() here — caller already accepted
        self.active_connections[case_id] = websocket
        log.info("WebSocket registered", case_id=case_id)

    def disconnect(self, case_id: str) -> None:
        self.active_connections.pop(case_id, None)
        log.info("WebSocket disconnected", case_id=case_id)

    async def send_panel_update(self, case_id: str, panel_name: str, data: dict) -> None:
        ws = self.active_connections.get(case_id)
        if ws:
            try:
                await ws.send_json({"panel": panel_name, "data": data})
            except Exception as e:
                log.warning("Failed to send panel update", case_id=case_id, error=str(e))

    async def subscribe_and_forward(self, case_id: str,
                                     websocket: WebSocket) -> None:
        try:
            r = await get_redis()
            pubsub = r.pubsub()

            # Subscribe FIRST before replaying
            await pubsub.subscribe(f"ws:{case_id}")

            # Replay panels already published
            sent = set()
            try:
                names = await r.lrange(
                    f"panels:{case_id}", 0, -1)
                for raw in names:
                    name = (raw.decode()
                            if isinstance(raw, bytes) else raw)
                    stored = await r.get(
                        f"panel:{case_id}:{name}")
                    if not stored:
                        continue
                    msg = (stored.decode()
                           if isinstance(stored, bytes)
                           else stored)
                    parsed = json.loads(msg)
                    await websocket.send_json(parsed)
                    sent.add(name)
                    log.info("Replayed panel",
                             case_id=case_id, panel=name)
                    if (name == "pipeline_complete"
                            or parsed.get("type") == "pipeline_complete"):
                        return
            except Exception as e:
                log.warning("Replay error",
                            case_id=case_id, error=str(e))

            # Listen for new messages
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                raw = message.get("data", "")
                try:
                    parsed = json.loads(raw)
                    panel = parsed.get(
                        "panel", parsed.get("type", ""))
                    if panel in sent:
                        continue
                    await websocket.send_json(parsed)
                    sent.add(panel)
                    if parsed.get("type") == "pipeline_complete":
                        break
                except Exception as e:
                    log.warning("Forward error", error=str(e))

        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.warning("WS error", case_id=case_id, error=str(e))
        finally:
            try:
                await pubsub.unsubscribe(f"ws:{case_id}")
            except Exception:
                pass


manager = ConnectionManager()
