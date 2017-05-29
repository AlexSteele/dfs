
import socket
import select

import rpc
from constants import *

class ChunkInfo:

    def __init__(self, id, addr):
        self.id = id
        self.addr = addr

    def to_hash(self):
        return {
            "id": self.id,
            "addr": self.addr
        }

    @staticmethod
    def from_hash(h):
        host = h["addr"][0]
        port = int(h["addr"][1])
        addr = (host, port)
        ci = ChunkInfo(
            id=h["id"],
            addr=addr
        )
        return ci

class FileInfo:

    def __init__(self):
        self.chunk_info = []
        self.nopen = 0
        self.deleted = False

    def to_hash(self):
        return {
            "chunk_info": [cinfo.to_hash() for cinfo in self.chunk_info],
        }

    @staticmethod
    def from_hash(h):
        fi = FileInfo()
        fi.chunk_info = [ChunkInfo.from_hash(ch) for ch in h["chunk_info"]]
        return fi

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
            except Exception as err:
                if self._idx is start:
                    raise ValueError("No chunk servers up.")

class Master:

    # TODO: Handle disconnect of chunk servers.

    def __init__(self,
                 chunkserver_iter,
                 chunkservers=[],
                 file_info={}):
        self._chunkservers = chunkservers
        self._chunkserver_dict = {}
        self._chunkserver_iter = chunkserver_iter
        self._file_info = file_info

        for cserver in chunkservers:
            self._chunkserver_dict[cserver.addr()] = cserver

    def _get_file_info(self, fname):
        info = self._file_info.get(fname, None)
        if not info or info.deleted:
            raise IOError("No such file {}".format(fname))
        return info

    def _do_delete(self, fname, finfo):
        # TODO: Do async.
        del self._file_info[fname]
        for cinfo in finfo.chunk_info:
            cserver = self._chunkserver_dict[cinfo.addr]
            cserver.delete_chunk(cinfo.id)

    def create(self, fname):
        if fname in self._file_info:
            raise IOError("File {} already exists.".format(fname))
        info = FileInfo()
        self._file_info[fname] = info
        return info

    def delete(self, fname):
        info = self._get_file_info(fname)
        if info.nopen > 0:
            info.deleted = True
            return
        self._do_delete(fname, info)
    
    def open(self, fname):
        info = self._get_file_info(fname)
        info.nopen += 1
        return info

    def close(self, fname):
        info = self._get_file_info(fname)
        info.nopen -= 1
        if info.deleted and info.nopen is 0:
            self._do_delete(fname, info)

    def request_new_chunk(self, fname):
        import uuid

        finfo = self._get_file_info(fname)
        server = self._chunkserver_iter.next(self._chunkservers)
        chunkid = str(uuid.uuid4())
        server.create_chunk(chunkid)
        cinfo = ChunkInfo(
            id=chunkid,
            addr=server.addr()
        )
        finfo.chunk_info.append(cinfo)
        return cinfo

    def get_chunk_info(self, fname,
                       start_idx=-1,
                       end_idx=-1):
        info = self._get_file_info(fname)
        start_idx = 0 if start_idx is -1 else start_idx
        end_idx = len(info.chunk_info) if end_idx is -1 else end_idx
        return info.chunk_info[start_idx:end_idx]

    def add_chunk_server(self, server):
        self._chunkservers.append(server)
        self._chunkserver_dict[server.addr()] = server

class RemoteMaster:

    def __init__(self, conn):
        self._conn = conn

    def _check_error(self, resp):
        if "error" in resp:
            raise IOError(resp["error"])

    def create(self, fname):
        resp = rpc.call_sync(
            conn=self._conn,
            method="create",
            fname=fname
        )
        self._check_error(resp)
        return FileInfo.from_hash(resp)

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
            method="open",
            fname=fname
        )
        self._check_error(resp)
        return FileInfo.from_hash(resp)

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

    # def get_chunk_info(self, fname,
    #                    start_idx=-1,
    #                    end_idx=-1):
    #     resp = rpc.call_sync(
    #         conn=self._conn,
    #         method="get_chunk_info",
    #         fname=fname,
    #         start_idx=start_idx,
    #         end_idx=end_idx
    #     )
    #     self._check_error(resp)
    #     return
    #     infos = [ChunkInfo.from_hash(h) for h in resp["info"]]
    #     return infos

    def ping(self):
        resp = rpc.call_sync(self._conn, "ping")
        assert(resp["status"] == "OK")

    # TODO: Rename.
    def closeconn(self):
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
    from chunkserver import RemoteChunkServer

    client_port = DEFAULT_MASTER_CLIENT_PORT
    chunk_port = DEFAULT_MASTER_CHUNK_PORT

    args = sys.argv[1:]
    for idx, arg in enumerate(args):
        if arg == "--client-port" and idx+1 < len(args):
            client_port = int(args[idx+1])
        elif arg == "--chunk-port" and idx+1 < len(args):
            chunk_port = int(args[idx+1])

    print("Starting master server.")

    client_listener = _listen(client_port)
    print("Listening for clients on port {}".format(client_port))

    chunk_listener = _listen(chunk_port)
    print("Listening for chunkservers on port {}".format(chunk_port))

    readers = [client_listener, chunk_listener]
    master = Master(chunkserver_iter=RoundRobinIter())
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
                    finfo = master.create(msg["fname"])
                    resp = finfo.to_hash()
                except Exception as err:
                    resp["error"] = str(err)
            elif method == "delete":
                try:
                    master.delete(msg["fname"])
                except Exception as err:
                    print(err)
                    resp["error"] = str(err)
            elif method == "open":
                try:
                    finfo = master.open(msg["fname"])
                    resp = finfo.to_hash()
                except Exception as err:
                    print(err)
                    resp["error"] = str(err)
            elif method == "close":
                try:
                    master.close(msg["fname"])
                except Exception as err:
                    print(err)
                    resp["error"] = str(err)
            elif method == "request_new_chunk":
                try:
                    cinfo = master.request_new_chunk(msg["fname"])
                    resp = cinfo.to_hash()
                except Exception as err:
                    print(err)
                    resp["error"] = str(err)
            elif method == "get_chunk_info":
                try:
                    infos = master.get_chunk_info(
                        msg["fname"],
                        msg["start_offset"],
                        msg["end_offset"]
                    )
                    resp["chunk_info"] = [info.to_hash() for info in infos]
                except Exception as err:
                    print(err)
                    resp["error"] = str(err)
            elif method == "ping":
                resp["status"] = "OK"
            else:
                print("Unrecognized RPC call.")
                resp["error"] = "Unrecognized RPC method {}".format(method)

            rpc.sendmsg(s, resp)

if __name__ == "__main__":
    main()
