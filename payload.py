import random
import asyncio
import time

import asyncpg
import json
import settings
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class PayloadGenerator:
    def __init__(self, conn):
        self.conn = conn

    @staticmethod
    def rand_payload() -> dict:
        payload = {}
        if random.randint(0, 10) > 5:
            payload = {
                "value": random.randint(0, 1000),
                "score": round(random.random(), 2),
                "ocr_score": round(random.random(), 2),
                "bounding_box": [round(random.random(), 2) for i in range(4)],

            }
        return payload

    def generate(self) -> dict:
        choices = [
            [self.rand_payload() for _ in range(0, 5)],
            [self.rand_payload() for _ in range(0, 9)],
        ]
        payload = {"business_id": random.randint(0, 9), "timestamp": time.time(), "version": "0.0.0.1"}
        fields = ["total", "line_items"]
        for f in fields:
            payload[f] = choices[random.randint(0, 9) % len(fields)]
        logging.info(payload)
        return payload

    @classmethod
    async def create(cls):
        conn = await asyncpg.connect(settings.settings.pg_connection_string)
        return PayloadGenerator(conn)

    async def next_step(self):
        payload = self.generate()

        await self.conn.execute('''INSERT INTO documents (ml_response) values($1);''', json.dumps(payload))
        await asyncio.sleep(3)


async def async_loop():
    payload_generator = await PayloadGenerator.create()
    while True:
        await payload_generator.next_step()


loop = asyncio.get_event_loop()
loop.run_until_complete(async_loop())
