
import socket
import select

import rpc
from constants import *

class ChunkInfo:

    def __init__(self, id, chunk_server_addr):
        self.id = id
        self.addr = chunk_server_addr

    def to_hash(self):
        return {
            "id": self.id,
            "addr": self.addr
        }

    def from_hash(h):
        ci = ChunkInfo()
        ci.id = h["id"]
        ci.addr = h["addr"]
        return ci

class FileInfo:

    def __init__(self):
        self.chunk_info = []
        self.nopen = 0
        self.deleted = False

class RoundRobinIter:

    def __init__(self):
        self._idx = 0

    def next(self, chunk_servers):
        start = self._idx
        while True:
            s = chunk_servers[self._idx]
            self._idx += 1
            self._idx %= len(chunk_servers)
            try:
                s.ping()
                return s
            except ValueError as err:
                if self._idx is start:
                    raise ValueError("No chunk servers up.")

class Master:

    # TODO: Handle disconnect of chunk servers.

    def __init__(self,
                 chunk_server_iter,
                 chunk_servers=[],
                 file_info={}):
        self._chunk_servers = chunk_servers
        self._cserver_iter = chunk_server_iter
        self._file_info = file_info

    def _get_info(self, fname):
        info = self._file_info.get(fname, None)
        if not info:
            raise ValueError("No such file {}".format(fname))
        return info

    def _del_file(fname, finfo):
        # TODO: Do async.
        del self._file_info[fname]
        for cinfo in finfo.chunk_info:
            cserver = self._chunk_servers[cinfo.addr]
            cserver.delete_chunk(cinfo.id)

    def create(self, fname):
        if fname in self._file_info:
            raise ValueError("File {} already exists.".format(fname))
        info = FileInfo()
        self._file_info[fname] = info

    def delete(self, fname):
        info = self._get_info(fname)
        if info.nopen > 0:
            info.deleted = True
            return
        self._del_file(fname, info)

    def open(self, fname):
        info = self._get_info(fname)
        info.nopen += 1

    def close(self, fname):
        info = self._get_info(fname)
        info.nopen -= 1
        if info.deleted and info.nopen is 0:
            self._del_file(fname, info)
    
    def request_new_chunk(self, fname):
        import uuid
        
        finfo = self._get_info(fname)
        server = self._cserver_iter.next(self._chunk_servers)
        cid = uuid.uuid4()
        server.create_chunk(cid)
        cinfo = ChunkInfo()
        cinfo.id = cid
        cinfo.addr = server.addr()
        return cinfo

    def get_chunk_info(self, fname,
                       start_idx=-1,
                       end_idx=-1):
        info = self._get_info(fname)
        start_idx = 0 if start_idx is -1 else start_idx
        end_idx = len(info.chunk_info) if end_idx is -1 else end_idx
        return info.chunk_info[start_idx:end_idx]

    def add_chunk_server(self, server):
        self._chunk_servers.append(server)

class RemoteMaster:

    def __init__(self, conn):
        self._conn = conn

    def _check_error(self, resp):
        if "error" in resp:
            raise ValueError(resp["error"])

    def create(self, fname):
        resp = rpc.call_sync(
            conn=self._conn,
            method="create",
            fname=fname
        )
        self._check_error(resp)

    def delete(self, fname):
        resp = rpc.call_sync(
            conn=self._conn,
            method="delete",
            fname=fname
        )
        self._check_error(resp)

    def open(self, fname):
        resp = rpc.call_sync(
            conn=self._conn,
            method="delete",
            fname=fname
        )
        self._check_error(resp)        

    def close(self, fname):
        resp = rpc.call_sync(
            conn=self._conn,
            method="close",
            fname=fname
        )
        self._check_error(resp)

    def request_new_chunk(self, fname):
        resp = rpc.call_sync(
            conn=self._conn,
            method="request_new_chunk",
            fname=fname
        )
        self._check_error(resp)
        return ChunkInfo.from_hash(resp)

    def get_chunk_info(self, fname,
                       start_idx=-1,
                       end_idx=-1):
        resp = rpc.call_sync(
            conn=self._conn,
            method="get_chunk_info",
            fname=fname,
            start_idx=start_idx,
            end_idx=end_idx
        )
        self._check_error(resp)        
        infos = [ChunkInfo.from_hash(h) for h in resp["info"]]
        return infos            

    def ping(self):
        resp = rpc.call_sync(self._conn, "ping")
        assert(resp["status"] == "OK")

    def close(self):
        self._conn.close()

def connect(addr):
    conn = socket.socket()
    conn.connect(addr)
    return RemoteMaster(conn)

def _listen(port):
    l = socket.socket()
    l.bind(("", port))
    l.listen(1)
    return l

def main():
    import sys
    from chunk_server import RemoteChunkServer
    
    client_port = DEFAULT_MASTER_CLIENT_PORT
    chunk_port = DEFAULT_MASTER_CHUNK_PORT

    args = sys.argv[1:]
    for idx, arg in enumerate(args):
        if arg == "--client-port" and idx+1 < len(args):
            client_port = int(args[idx+1])
        elif arg == "--chunk-port" and idx+1 < len(args):
            chunk_port = int(args[idx+1])

    client_listener = _listen(client_port)
    print("Listening for clients on port {}".format(client_port))

    chunk_listener = _listen(chunk_port)
    print("Listening for chunkservers on port {}".format(chunk_port))

    readers = [client_listener, chunk_listener]
    master = Master(chunk_server_iter=RoundRobinIter())
    while True:
        rlist, _, _ = select.select(readers, [], [])
        for s in rlist:
            
            if s is client_listener:
                conn, addr = client_listener.accept()
                print("Accepted client conn with {}".format(addr))
                readers.append(conn)
                continue

            if s is chunk_listener:
                conn, addr = chunk_listener.accept()
                print("Accepted chunk conn with {}".format(addr))
                cserver = RemoteChunkServer(conn)
                master.add_chunk_server(cserver)
                continue
            
            msg = rpc.recvmsg(s)
            if not msg:
                print("Client {} closed conn.".format(s.getpeername()))
                s.close()
                readers.remove(s)
                continue

            print("RPC call: {}".format(msg))

            method = msg["method"]
            resp = {}
            if method == "create":
                try:
                    master.create(msg["fname"])
                except ValueError as err:
                    resp["error"] = str(err)
            elif method == "delete":
                try:
                    master.delete(msg["fname"])
                except ValueError as err:
                    resp["error"] = str(err)
            elif method == "open":
                try:
                    _master.open(msg["fname"])
                except ValueError as err:
                    resp["error"] = str(err)
            elif method == "close":
                try:
                    master.close(msg["fname"])
                except ValueError as err:
                    resp["error"] = str(err)                    
            elif method == "request_new_chunk":
                try:
                    cinfo = master.request_new_chunk(msg["fname"])
                    resp["chunk_info"] = cinfo.to_hash()
                except ValueError as err:
                    resp["error"] = str(err)
            elif method == "get_chunk_info":
                try:
                    infos = master.get_chunk_info(
                        msg["fname"],
                        msg["start_offset"],
                        msg["end_offset"]
                    )
                    resp["chunk_info"] = [info.to_hash() for info in infos]
                except ValueError as err:
                    resp["error"] = str(err)
            elif method == "ping":
                resp["status"] = "OK"
            else:
                print("Unrecognized RPC call.")
                resp["error"] = "Unrecognized RPC method {}".format(method)

            rpc.sendmsg(s, resp)

if __name__ == "__main__":
    main()
