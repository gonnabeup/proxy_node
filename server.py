import asyncio
import json
import logging
from typing import Dict, Set, Optional
from aiohttp import web
from .config import PROXY_HOST, PROXY_API_HOST, PROXY_API_PORT, PROXY_API_TOKEN, PROXY_NODE_NAME
from .models import init_db, get_session, User, Mode, UserPort

logger = logging.getLogger(__name__)

class ProxyNode:
    def __init__(self, host: str = PROXY_HOST):
        self.host = host
        self._engine = init_db()
        self._servers: Dict[int, asyncio.AbstractServer] = {}
        self._clients: Dict[int, Set[asyncio.Task]] = {}
        self._port_mode: Dict[int, dict] = {}
        self._lock = asyncio.Lock()
        self._running: bool = False
        self._http_runner: Optional[web.AppRunner] = None
        self._http_site: Optional[web.TCPSite] = None

    async def start(self):
        self._running = True
        s = get_session(self._engine)
        try:
            ports = s.query(UserPort).filter((UserPort.proxy_node == PROXY_NODE_NAME) | (UserPort.proxy_node == None)).all()
            for up in ports:
                await self._start_port(up.port)
        finally:
            s.close()

    async def stop(self):
        self._running = False
        for p in list(self._servers.keys()):
            await self._stop_port(p)
        await self.stop_http_api()

    async def reload_port(self, port: int):
        async with self._lock:
            await self._stop_port(port)
            await self._start_port(port)

    async def start_port(self, port: int):
        async with self._lock:
            await self._start_port(port)

    async def stop_port(self, port: int):
        async with self._lock:
            await self._stop_port(port)

    async def _start_port(self, port: int):
        s = get_session(self._engine)
        try:
            up = s.query(UserPort).filter(UserPort.port == port).first()
            u = s.query(User).filter(User.id == up.user_id).first() if up else None
            if not u:
                return
            m = s.query(Mode).filter(Mode.user_id == u.id, Mode.is_active == 1).first()
            if m:
                self._port_mode[port] = {"host": m.host, "port": m.port, "alias": m.alias, "mode_name": m.name, "login": u.login}
            else:
                self._port_mode[port] = {"host": "sleep", "port": 0, "alias": "", "mode_name": "sleep", "login": u.login}
        finally:
            s.close()
        if port in self._servers:
            return
        server = await asyncio.start_server(lambda r, w: self._handle_client(r, w, port), self.host, port)
        self._servers[port] = server
        self._clients.setdefault(port, set())

    async def _stop_port(self, port: int):
        srv = self._servers.pop(port, None)
        if srv:
            srv.close()
            await srv.wait_closed()
        tasks = self._clients.pop(port, set())
        for t in list(tasks):
            t.cancel()
        self._port_mode.pop(port, None)

    async def start_http_api(self, host: str = PROXY_API_HOST, port: int = PROXY_API_PORT, token: Optional[str] = PROXY_API_TOKEN):
        app = web.Application()
        async def _auth(request):
            t = token or ""
            if t and request.headers.get("X-Proxy-Token", "") != t:
                return web.json_response({"error": "unauthorized"}, status=401)
            return None
        async def health(request):
            err = await _auth(request)
            if err:
                return err
            return web.json_response({"status": "ok"})
        async def status(request):
            err = await _auth(request)
            if err:
                return err
            return web.json_response({"ports": sorted(list(self._servers.keys()))})
        async def reload_port_handler(request):
            err = await _auth(request)
            if err:
                return err
            data = await request.json()
            p = int(data.get("port"))
            await self.reload_port(p)
            return web.json_response({"result": "reloaded", "port": p})
        async def start_port_handler(request):
            err = await _auth(request)
            if err:
                return err
            data = await request.json()
            p = int(data.get("port"))
            await self.start_port(p)
            return web.json_response({"result": "started", "port": p})
        async def stop_port_handler(request):
            err = await _auth(request)
            if err:
                return err
            data = await request.json()
            p = int(data.get("port"))
            await self.stop_port(p)
            return web.json_response({"result": "stopped", "port": p})
        app.add_routes([
            web.get("/health", health),
            web.get("/status", status),
            web.post("/reload-port", reload_port_handler),
            web.post("/start-port", start_port_handler),
            web.post("/stop-port", stop_port_handler),
        ])
        self._http_runner = web.AppRunner(app)
        await self._http_runner.setup()
        self._http_site = web.TCPSite(self._http_runner, host, port)
        await self._http_site.start()

    async def stop_http_api(self):
        if self._http_site:
            await self._http_site.stop()
            self._http_site = None
        if self._http_runner:
            await self._http_runner.cleanup()
            self._http_runner = None

    async def _handle_client(self, miner_reader: asyncio.StreamReader, miner_writer: asyncio.StreamWriter, port: int):
        task = asyncio.current_task()
        self._clients.setdefault(port, set()).add(task)
        s = get_session(self._engine)
        try:
            up = s.query(UserPort).filter(UserPort.port == port).first()
            u = s.query(User).filter(User.id == up.user_id).first() if up else None
            m = s.query(Mode).filter(Mode.user_id == u.id, Mode.is_active == 1).first() if u else None
            if u and m:
                self._port_mode[port] = {"host": m.host, "port": m.port, "alias": m.alias, "mode_name": m.name, "login": u.login}
            elif u:
                self._port_mode[port] = {"host": "sleep", "port": 0, "alias": "", "mode_name": "sleep", "login": u.login}
        finally:
            s.close()
        cached = self._port_mode.get(port)
        if not cached or cached.get("mode_name") == "sleep" or not cached.get("host") or int(cached.get("port", 0)) == 0:
            try:
                msg = {"id": None, "result": None, "error": {"code": -1, "message": "proxy sleep"}}
                miner_writer.write((json.dumps(msg) + "\n").encode())
                await miner_writer.drain()
            except Exception:
                pass
            miner_writer.close()
            await miner_writer.wait_closed()
            self._clients.get(port, set()).discard(task)
            return
        host = cached.get("host")
        upstream_port = int(cached.get("port"))
        alias_login = cached.get("alias", "")
        try:
            pool_reader, pool_writer = await asyncio.open_connection(host, upstream_port)
        except Exception:
            miner_writer.close()
            try:
                await miner_writer.wait_closed()
            except Exception:
                pass
            self._clients.get(port, set()).discard(task)
            return
        async def forward_to_pool():
            try:
                while not miner_reader.at_eof():
                    data = await miner_reader.readline()
                    if not data:
                        break
                    text = data.decode(errors='ignore').strip()
                    if not text:
                        continue
                    try:
                        msg = json.loads(text)
                    except json.JSONDecodeError:
                        pool_writer.write(data)
                        await pool_writer.drain()
                        continue
                    method = msg.get("method")
                    if method == "mining.authorize":
                        params = msg.get("params", [])
                        if params and isinstance(params[0], str) and alias_login:
                            original = params[0]
                            if "." in original:
                                miner_login, worker = original.split(".", 1)
                            else:
                                miner_login, worker = original, ""
                            new_user = f"{alias_login}.{worker}" if worker else alias_login
                            msg["params"][0] = new_user
                        data = (json.dumps(msg) + "\n").encode()
                    pool_writer.write(data)
                    await pool_writer.drain()
            except Exception:
                pass
        async def forward_to_miner():
            try:
                while not pool_reader.at_eof():
                    data = await pool_reader.readline()
                    if not data:
                        break
                    miner_writer.write(data)
                    await miner_writer.drain()
            except Exception:
                pass
        t1 = asyncio.create_task(forward_to_pool())
        t2 = asyncio.create_task(forward_to_miner())
        try:
            await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)
        finally:
            try:
                pool_writer.close()
                await pool_writer.wait_closed()
            except Exception:
                pass
            try:
                miner_writer.close()
                await miner_writer.wait_closed()
            except Exception:
                pass
            self._clients.get(port, set()).discard(task)
