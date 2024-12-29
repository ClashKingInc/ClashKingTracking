import asyncio
from collections import defaultdict, deque

import orjson
import ujson
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, WebSocket
from loguru import logger
from uvicorn import Config, Server

CLAN_MAP = defaultdict(set)
MISSED_EVENTS = defaultdict(lambda: deque(maxlen=10_000))
app = FastAPI()
CONNECTED_CLIENTS = {}

@app.websocket('/events')
async def event_websocket(websocket: WebSocket):
    global CLAN_MAP
    global MISSED_EVENTS
    global CONNECTED_CLIENTS

    await websocket.accept()
    await websocket.send_text('Successfully Login!')
    try:
        while True:
            data: bytes = await websocket.receive_bytes()
            data: dict = orjson.loads(data)
            clans = data.get('clans', [])
            client_id = data.get('client_id')
            if client_id is None:
                await websocket.close()
                return

            CONNECTED_CLIENTS[client_id] = websocket
            for clan in clans:
                CLAN_MAP[client_id].add(clan)

            # Send missed events if any exist
            if client_id in MISSED_EVENTS and MISSED_EVENTS[client_id]:
                while MISSED_EVENTS[client_id]:
                    missed_event = MISSED_EVENTS[client_id].popleft()
                    await websocket.send_json(missed_event)
    except Exception as e:
        await websocket.close()

async def broadcast():
    global CLAN_MAP

    async def send_ws(client_id, ws, json):
        try:
            if ws is not None:
                await ws.send_json(json)
            else:
                MISSED_EVENTS[client_id].append(json)
        except Exception as e:
            logger.error(e)
            for client_id, websocket in CONNECTED_CLIENTS.copy().items():
                if websocket == ws:
                    CONNECTED_CLIENTS[client_id] = None

    topics = ['clan', 'player', 'war', 'capital', 'reminder', 'reddit', 'giveaway']
    consumer: AIOKafkaConsumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers='85.10.200.219:9092',
        auto_offset_reset='latest'
    )
    await consumer.start()
    logger.info('Events Started')
    async for msg in consumer:
        message_to_send = {'topic': msg.topic, 'value': ujson.loads(msg.value)}

        key = msg.key.decode('utf-8') if msg.key is not None else None
        tasks = []
        for client_id, client in CONNECTED_CLIENTS.items():   # type: str, WebSocket
            clans = CLAN_MAP.get(client_id, [])
            if key in clans or key is None or not clans:
                tasks.append(send_ws(client_id=client_id, ws=client, json=message_to_send))
        await asyncio.gather(*tasks)


async def main():
    loop = asyncio.get_event_loop()
    config = Config(
        app=app,
        loop='asyncio',
        host='0.0.0.0',
        port=8000,
        ws_ping_interval=120,
        ws_ping_timeout=120,
    )
    server = Server(config)
    loop.create_task(server.serve())
    loop.create_task(broadcast())
