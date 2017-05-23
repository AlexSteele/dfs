
import socket
import select

from constants import *
import rpc

class ChunkServer:

    def __init__(self, env):
        self._env = env

    def _chunk_fname(self, chunkid):
        return str(chunkid)

    def create_chunk(self, chunkid):
        fd = self._env.open(self._chunk_fname(chunkid), "w+")
        self._env.close(fd)

    def delete_chunk(self, chunkid):
        self._env.remove(self._chunk_fname(chunkid))

    def write_chunk(self, chunkid, data):
        # TODO: Check file exists.
        fd = self._env.open(self._chunk_fname(chunkid), "w+")
        self._env.write(fd, data)
        self._env.close(fd)

    def read_chunk(self, chunkid,
                   start_offset=-1,
                   end_offset=-1):
        fd = self._env.open(self._chunk_fname(chunkid), "r")
        
        if start_offset is -1 and end_offset is -1:
            return self._env.readall(fd)

        assert(start_offset is not -1)
        assert(end_offset is not -1)
        data = self._env.readrange(
            fd,
            start_offset=start_offset,
            end_offset=end_offset
        )
        return data

class RemoteChunkServer:

    def __init__(self, conn):
        self._conn = conn

    def _check_error(self, resp):
        if "error" in resp:
            raise ValueError(resp["error"])

    def create_chunk(self, chunkid):
        resp = rpc.call_sync(
            self._conn,
            "create_chunk",
            chunkid=chunkid
        )
        self._check_error(resp)

    def delete_chunk(self, chunkid):
        resp = rpc.call_sync(
            self._conn,
            "delete_chunk",
            chunkid=chunkid
        )
        self._check_error(resp)

    def write_chunk(self, chunkid, data):
        resp =  rpc.call_sync(
            self._conn,
            "write_chunk",
            chunkid=chunkid,
            data=data,
            offset=offset
        )
        self._check_error(resp)

    def read_chunk(self, chunkid,
                   start_offset=-1,
                   end_offset=-1):
        resp = rpc.call_sync(
            self._conn,
            "read_chunk",
            chunkid=chunkid,
            start_offset=start_offset,
            end_offset=end_offset
        )
        self._check_error(resp)
        return resp["chunk"]

    def close(self):
        self._conn.close()

    def addr(self):
        return self._conn.getpeername()

    def ping(self):
        resp = rpc.call_sync(self._conn, "ping")
        assert(resp["status"] == "ok")

def connect(addr):
    conn = socket.socket()
    conn.connect(addr)
    return RemoteChunkServer(conn)

def main():
    import sys
    import env

    args = sys.argv[1:]
    client_port = DEFAULT_CHUNK_SERVER_CLIENT_PORT
    master_addr = DEFAULT_CHUNK_SERVER_MASTER_ADDR
    for idx, arg in enumerate(args):
        if arg == "--client-port" and idx+1 < len(args):
            client_port = int(args[idx+1])
        if arg == "--master-addr" and idx+1 < len(args):
            l = args[idx+1].split(":")
            master_addr = (l[0], int(l[1]))

    master_conn = socket.socket()
    try:
        master_conn.connect(master_addr)
    except socket.error as err:
        print("Unable to connect to master.")
        sys.exit(1)
    print("Connected to master at address {}".format(master_addr))

    client_listener = socket.socket()
    client_listener.bind(("", client_port))
    client_listener.listen(1)
    print("Listening for clients on port {}".format(client_port))

    readers = [client_listener, master_conn]
    chunk_server = ChunkServer(env=env.MemEnv())
    while True:
        rlist, _, _ = select.select(readers, [], [])
        for s in rlist:
            
            if s is client_listener:
                conn, addr = s.accept()
                print("Accepted client conn with {}".format(addr))
                readers.append(conn)
                continue

            msg = rpc.recvmsg(s)
            if not msg:
                print("Client {} closed conn.".format(s.getpeername()))
                s.close()
                readers.remove(s)
                continue

            method = msg["method"]
            print("RPC Call to method {}".format(method))

            resp = {}
            if method == "create_chunk":
                try:
                    chunk_server.create_chunk(msg["chunkid"])
                except Exception as err:
                    resp["error"] = str(err)
            elif method == "delete_chunk":
                try:
                    chunk_server.delete_chunk(msg["chunkid"])
                except Exception as err:
                    resp["error"] = str(err)
            elif method == "write_chunk":
                try:
                    chunk_server.write_chunk(
                        msg["chunkid"],
                        msg["data"],
                        msg["offset"]
                    )
                except Exception as err:
                    resp["error"] = str(err)
            elif method == "read_chunk":
                try:
                    resp["chunk"] = chunk_server.read_chunk(msg["chunkid"])
                except Exception as err:
                    resp["error"] = str(err)
            elif method == "ping":
                resp["status"] = "ok"
            else:
                resp["error"] = "Unrecognized method: {}".format(method)
            rpc.sendmsg(s, resp)

if __name__ == "__main__":
    main()
