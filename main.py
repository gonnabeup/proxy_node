import asyncio
import logging
import io
from server import ProxyNode
from config import PROXY_API_HOST, PROXY_API_PORT, PROXY_API_TOKEN

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', handlers=[logging.StreamHandler(io.TextIOWrapper(__import__('sys').stdout.buffer, encoding='utf-8', errors='ignore'))])

async def main():
    srv = ProxyNode()
    await srv.start_http_api(PROXY_API_HOST, PROXY_API_PORT, PROXY_API_TOKEN)
    await srv.start()
    ev = asyncio.Event()
    await ev.wait()

if __name__ == '__main__':
    asyncio.run(main())
