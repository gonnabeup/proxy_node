import asyncio
import json
import logging
from typing import Dict, Set, Optional
from aiohttp import web
from .config import PROXY_HOST, PROXY_API_HOST, PROXY_API_PORT, PROXY_API_TOKEN, PROXY_NODE_NAME, CACHE_ENABLED
from .models import init_local_db, get_local_session, PortCache

logger = logging.getLogger(__name__)

class ProxyNode:
    def __init__(self, host: str = PROXY_HOST):
        self.host = host
        self._local_engine = init_local_db()
        self._servers: Dict[int, asyncio.AbstractServer] = {}
        self._clients: Dict[int, Set[asyncio.Task]] = {}
        self._port_mode: Dict[int, dict] = {}
        self._lock = asyncio.Lock()
        self._running: bool = False
        self._http_runner: Optional[web.AppRunner] = None
        self._http_site: Optional[web.TCPSite] = None

    async def start(self):
        self._running = True
        # Ensure local DB tables exist
        try:
            from sqlalchemy import inspect
            insp = inspect(self._local_engine)
            if not insp.has_table('port_cache'):
                PortCache.__table__.create(self._local_engine)
        except Exception:
            pass
        ls = get_local_session(self._local_engine)
        try:
            cached_ports = ls.query(PortCache).all()
            for cp in cached_ports:
                self._port_mode[cp.port] = {
                    "host": cp.host or "sleep",
                    "port": int(cp.upstream_port or 0),
                    "alias": cp.alias or "",
                    "mode_name": cp.mode_name or ("sleep" if not cp.host or not cp.upstream_port else ""),
                    "login": cp.login or "",
                }
                try:
                    await self._start_port(cp.port)
                except Exception:
                    pass
        finally:
            ls.close()

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
        # read local cache only
        ls = get_local_session(self._local_engine)
        try:
            cp = ls.query(PortCache).filter(PortCache.port == port).first()
            if not cp:
                return
            self._port_mode[port] = {
                "host": cp.host or "sleep",
                "port": int(cp.upstream_port or 0),
                "alias": cp.alias or "",
                "mode_name": cp.mode_name or ("sleep" if not cp.host or not cp.upstream_port else ""),
                "login": cp.login or "",
            }
        finally:
            ls.close()
        # write cache
        if CACHE_ENABLED:
            ls = get_local_session(self._local_engine)
            try:
                pc = ls.query(PortCache).filter(PortCache.port == port).first()
                if not pc:
                    pc = PortCache(port=port)
                    ls.add(pc)
                pm = self._port_mode.get(port, {})
                pc.host = pm.get("host")
                pc.upstream_port = int(pm.get("port") or 0)
                pc.alias = pm.get("alias")
                pc.login = pm.get("login")
                pc.mode_name = pm.get("mode_name")
                from datetime import datetime
                pc.updated_at = datetime.utcnow()
                ls.commit()
            finally:
                ls.close()
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
        if CACHE_ENABLED:
            ls = get_local_session(self._local_engine)
            try:
                pc = ls.query(PortCache).filter(PortCache.port == port).first()
                if pc:
                    # keep cache entry but mark as sleep
                    pc.host = "sleep"
                    pc.upstream_port = 0
                    pc.alias = ""
                    pc.mode_name = "sleep"
                    from datetime import datetime
                    pc.updated_at = datetime.utcnow()
                    ls.commit()
            finally:
                ls.close()

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
        async def set_port_mode_handler(request):
            err = await _auth(request)
            if err:
                return err
            data = await request.json()
            p = int(data.get("port"))
            host = data.get("host") or "sleep"
            upstream_port = int(data.get("upstream_port") or 0)
            alias = data.get("alias") or ""
            login = data.get("login") or ""
            mode_name = data.get("mode_name") or ("sleep" if not host or not upstream_port else "")
            ls = get_local_session(self._local_engine)
            try:
                cp = ls.query(PortCache).filter(PortCache.port == p).first()
                if not cp:
                    from datetime import datetime
                    cp = PortCache(port=p)
                    ls.add(cp)
                cp.host = host
                cp.upstream_port = upstream_port
                cp.alias = alias
                cp.login = login
                cp.mode_name = mode_name
                from datetime import datetime
                cp.updated_at = datetime.utcnow()
                ls.commit()
                # update in-memory and reload
                self._port_mode[p] = {
                    "host": cp.host or "sleep",
                    "port": int(cp.upstream_port or 0),
                    "alias": cp.alias or "",
                    "mode_name": cp.mode_name or ("sleep" if not cp.host or not cp.upstream_port else ""),
                    "login": cp.login or "",
                }
            finally:
                ls.close()
            await self.reload_port(p)
            return web.json_response({"result": "ok", "port": p})
        async def sync_ports_handler(request):
            err = await _auth(request)
            if err:
                return err
            data = await request.json()
            ports = data.get("ports") or []
            replace = bool(data.get("replace"))
            ls = get_local_session(self._local_engine)
            try:
                if replace:
                    ls.query(PortCache).delete()
                    ls.commit()
                for entry in ports:
                    p = int(entry.get("port"))
                    host = entry.get("host") or "sleep"
                    upstream_port = int(entry.get("upstream_port") or 0)
                    alias = entry.get("alias") or ""
                    login = entry.get("login") or ""
                    mode_name = entry.get("mode_name") or ("sleep" if not host or not upstream_port else "")
                    cp = ls.query(PortCache).filter(PortCache.port == p).first()
                    if not cp:
                        cp = PortCache(port=p)
                        ls.add(cp)
                    cp.host = host
                    cp.upstream_port = upstream_port
                    cp.alias = alias
                    cp.login = login
                    cp.mode_name = mode_name
                from datetime import datetime
                now = datetime.utcnow()
                for cp in ls.query(PortCache).all():
                    cp.updated_at = now
                ls.commit()
                # restart all ports according to new cache
                to_ports = [cp.port for cp in ls.query(PortCache).all()]
            finally:
                ls.close()
            # stop ports not in list
            for p in list(self._servers.keys()):
                if p not in to_ports:
                    await self.stop_port(p)
            # start required ports
            for p in to_ports:
                await self.start_port(p)
            return web.json_response({"result": "ok", "count": len(to_ports)})
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
            web.post("/set-port-mode", set_port_mode_handler),
            web.post("/sync-ports", sync_ports_handler),
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
        # no main DB; rely on in-memory or local cache
        cached = self._port_mode.get(port)
        if (not cached) and CACHE_ENABLED:
            # try local cache
            ls = get_local_session(self._local_engine)
            try:
                pc = ls.query(PortCache).filter(PortCache.port == port).first()
                if pc:
                    cached = {
                        "host": pc.host or "sleep",
                        "port": int(pc.upstream_port or 0),
                        "alias": pc.alias or "",
                        "mode_name": pc.mode_name or ("sleep" if not pc.host or not pc.upstream_port else ""),
                        "login": pc.login or "",
                    }
                    self._port_mode[port] = cached
            finally:
                ls.close()
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
