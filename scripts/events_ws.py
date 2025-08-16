import asyncio
from collections import defaultdict, deque

import orjson
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, WebSocket
from uvicorn import Config, Server

from .tracking import Tracking, TrackingType


class TrackingWebsocket(Tracking):
    def __init__(self):
        super().__init__(tracker_type=TrackingType.WEBSOCKET)
        self.CONNECTED_CLIENTS = {}
        self.CLAN_MAP = defaultdict(set)
        self.MISSED_EVENTS = defaultdict(lambda: deque(maxlen=10_000))
        self.app = FastAPI()

        self.app.add_event_handler("startup", self._on_startup)
        self.app.add_websocket_route("/events", self._event_websocket)

        self.topics = ["clan", "player", "war", "capital", "reminder", "reddit", "giveaway"]

        self.consumer: AIOKafkaConsumer = ...

    async def _on_startup(self):
        await self.initialize()
        self.consumer: AIOKafkaConsumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.config.kafka_host,
            client_id="tracking_websocket",
            auto_offset_reset="latest",
            api_version="auto",
        )

        asyncio.create_task(self._broadcast())

    async def _event_websocket(self, websocket: WebSocket):
        await websocket.accept()
        await websocket.send_text("Successfully Login!")
        try:
            while True:
                data: bytes = await websocket.receive_bytes()
                data: dict = orjson.loads(data)
                clans = data.get("clans", [])
                client_id = data.get("client_id")
                if client_id is None:
                    await websocket.close()
                    return
                await websocket.send_text(f"Login! with clans: {clans} and id: {client_id}")

                self.CONNECTED_CLIENTS[client_id] = websocket
                self.CLAN_MAP[client_id] = set(clans)

                # Send missed events if any exist
                while self.MISSED_EVENTS.get(client_id):
                    missed_event = self.MISSED_EVENTS[client_id].popleft()
                    await websocket.send_json(missed_event)
        except Exception as e:
            self.logger.error(e)
            pass

    async def _send_ws(self, client_id, ws, json):
        try:
            if ws is not None:
                await ws.send_json(json)
            else:
                self.MISSED_EVENTS[client_id].append(json)
        except Exception as e:
            self.logger.error(e)
            for client_id, websocket in self.CONNECTED_CLIENTS.copy().items():
                if websocket == ws:
                    self.CONNECTED_CLIENTS[client_id] = None

    async def _broadcast(self):
        await self.consumer.start()
        self.logger.info("Events Started")
        async for msg in self.consumer:
            message_to_send = {"topic": msg.topic, "value": orjson.loads(msg.value)}
            key = msg.key.decode("utf-8") if msg.key is not None else None
            tasks = []
            for client_id, client in self.CONNECTED_CLIENTS.items():  # type: str, WebSocket
                clans = self.CLAN_MAP.get(client_id, [])
                if key in clans or key is None or not clans:
                    tasks.append(self._send_ws(client_id=client_id, ws=client, json=message_to_send))
            await asyncio.gather(*tasks)

    async def run(self):
        config = Config(
            app=self.app, loop="asyncio", host="0.0.0.0", port=8000, ws_ping_interval=120, ws_ping_timeout=120
        )
        server = Server(config)
        await server.serve()
