"""Compare fresh connections with run.py's persistent-connection baseline."""
import asyncio
import json

import aiohttp
import run


async def main():
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(force_close=True)) as session:
        def validate(text):
            assert json.loads(text.strip().splitlines()[-1]) == [100000, 4999950000]
        for clients in [1, 8, 32]:
            for repetition in [1, 2, 3]:
                await run.measure(session, 'house-aggregate-new-connection', run.HOUSE + '/',
                                  'SELECT count(), sum(id) FROM bench FORMAT JSONCompactEachRowWithNamesAndTypes',
                                  validate, clients, repetition)


if __name__ == '__main__':
    asyncio.run(main())
