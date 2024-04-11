import ujson
import orjson
from aiokafka import AIOKafkaConsumer
from uvicorn import Config, Server
from fastapi import FastAPI, WebSocket
from collections import defaultdict
import asyncio
from loguru import logger


EVENT_CLIENTS = set()
CLAN_MAP = defaultdict(set)
app = FastAPI()


@app.websocket("/events")
async def event_websocket(websocket: WebSocket):
    global EVENT_CLIENTS
    global CLAN_MAP
    await websocket.accept()
    EVENT_CLIENTS.add(websocket)
    await websocket.send_text("Successfully Login!")
    try:
        while True:
            data: bytes = await websocket.receive_bytes()
            data: dict = orjson.loads(data)
            clans = data.get("clans", [])
            for clan in clans:
                CLAN_MAP[websocket.client].add(clan)
    except Exception:
        EVENT_CLIENTS.remove(websocket)


async def broadcast():
    global EVENT_CLIENTS
    global CLAN_MAP
    async def send_ws(ws, json):
        try:
            await ws.send_json(json)
        except Exception as e:
            logger.error(e)
            EVENT_CLIENTS.discard(ws)


    topics = ["clan", "player", "war", "capital", "reminder", "reddit"]
    consumer: AIOKafkaConsumer = AIOKafkaConsumer(*topics, bootstrap_servers='85.10.200.219:9092', auto_offset_reset="latest")
    await consumer.start()
    logger.info("Events Started")
    async for msg in consumer:
        message_to_send = {
            "topic" : msg.topic,
            "value" : ujson.loads(msg.value)
        }

        key = msg.key.decode("utf-8") if msg.key is not None else None
        tasks = []
        for client in EVENT_CLIENTS.copy(): #type: WebSocket
            clans = CLAN_MAP.get(client.client, [])
            if key in clans or key is None:
                tasks.append(send_ws(ws=client, json=message_to_send))
        await asyncio.gather(*tasks)


async def main():
    loop = asyncio.get_event_loop()
    config = Config(app=app, loop="asyncio", host="0.0.0.0", port=8000, ws_ping_interval=120 ,ws_ping_timeout= 120)
    server = Server(config)
    loop.create_task(server.serve())
    loop.create_task(broadcast())
