
import datetime
import json
import socket
import struct
import select

MASTER_PORT = 8888
CHUNK_SERVER_PORT = 8001

def log(s):
    print(s)

def recvn(conn, n):
    msg = ""
    while len(msg) < n:
        r = conn.recv(n - len(msg))
        if not r:
            return None
        msg += r
    return msg

def sendall(conn, msg):
    n = 0
    l = len(msg)
    while n < l:
        n += conn.send(msg[n:])

def sendmsg(conn, msg):
    msgstr = json.dumps(msg)
    lenstr = struct.pack("i", len(msgstr))
    sendall(conn, lenstr)
    sendall(conn, msgstr)

def recvmsg(conn):
    lenstr = recvn(conn, 4)
    if not lenstr:
        return None
    msglen = struct.unpack("i", lenstr)[0]
    msgstr = recvn(conn, msglen)
    if not msgstr:
        return None
    msg = json.loads(msgstr)
    return msg

def rpc_call(conn, method, **args):
    args["method"] = method
    sendmsg(conn, args)
    resp = recvmsg(conn)
    return resp

CHUNK_SIZE = 1 << 26

class Master:

    def __init__(self,
                 file_info=None,
                 chunk_servers=None):
        
        file_info = file_info or {}
        chunk_servers = chunk_servers or []

        self._file_info = file_info
        self._chunk_servers = chunk_servers

    def create(self, fname):
        
        if fname in self._file_info:
            return {
                "created": False
            }

        self._file_info[fname] = FileInfo()

        return {
            "created": True
        }

    def delete(self, fname):

        if self._file_info.pop(fname, None):
            return {
                "deleted": True
            }

        return {
            "deleted": False
        }

    def stat(self, fname):
        return {"hoorah": "yes"}

    def get_chunk_info(self, fname):
        return {"chunk": "info"}

    def request_chunk(self, fname):
        pass

class RemoteMaster:

    def __init__(self, conn):
        self._conn = conn

    def create(self, fname):
        return rpc_call(self._conn, "create", fname=fname)

    def stat(self, fname):
        return rpc_call(self._conn, "stat", fname=fname)

    def get_chunk_info(self, fname):
        return rpc_call(self._conn, "get_chunk_info", fname=fname)

def connect_master(addr):
    conn = socket.socket()
    conn.connect(addr)
    return RemoteMaster(conn)

def handle_client(conn, master):
    while True:
        msg = recvmsg(conn)
        if not msg:
            break
        method = msg["method"]
        resp = None
        if method == "create":
            resp = master.create(msg["fname"])
        elif method == "stat":
            resp = master.stat(msg["fname"])
        elif method == "get_chunk_info":
            resp = master.get_chunk_info(msg["fname"])
        sendmsg(conn, resp)

def master_main():
    sock = socket.socket()
    sock.bind(("", MASTER_PORT))
    sock.listen(10)
    log("Listening on port {}".format(MASTER_PORT))
    master = Master()
    while True:
        conn, addr = sock.accept()
        log("Accepted conn with {}".format(addr))
        handle_client(conn, master)

class ChunkServer:

    def __init__(self):
        pass

    def create_chunk(self, chunkid):
        return {"created": "yes"}

    def delete_chunk(self, chunkid):
        pass

    def write_chunk(self, chunkid, data, offset=-1):
        pass

    def read_chunk(self, chunkid):
        pass

    def status(self):
        pass

class RemoteChunkServer:

    def __init__(self, conn):
        self._conn = conn

    def create_chunk(self, chunkid):
        return rpc_call(self._conn, "create_chunk", chunkid=chunkid)

def connect_chunk_server(addr):
    conn = socket.socket()
    conn.connect(addr)
    return RemoteChunkServer(conn)

def chunk_server_main():
    chunk_server = ChunkServer()
    
    listener = socket.socket()
    listener.bind(("", CHUNK_SERVER_PORT))
    listener.listen(1)
    log("Listening on port {}".format(CHUNK_SERVER_PORT))

    readers = [listener]
    while True:
        rlist, _, _ = select.select(readers, [], [])
        for s in rlist:
            if s is listener:
                conn, addr = listener.accept()
                log("Accepted client conn with {}".format(addr))
                readers.append(conn)
                continue
            msg = recvmsg(s)
            if not msg:
                # TODO: Remove socket from list
                continue
            method = msg["method"]
            resp = None
            if method == "create_chunk":
                resp = chunk_server.create_chunk(msg["chunkid"])
            elif method == "delete_chunk":
                resp = chunk_server.delete_chunk(msg["chunkid"])
            elif method == "write_chunk":
                resp = chunk_server.write_chunk(msg["chunkid"], msg["data"], msg["offset"])
            elif method == "read_chunk":
                resp = chunk_server.read_chunk(msg["chunkid"])
            elif method == "status":
                resp = chunk_server.status()
            sendmsg(s, resp)

class ChunkInfo:

    def __init__(self, id, chunk_server_addr):
        self.id = id
        self.server_addr = chunk_server_addr

class FileInfo:

    def __init__(self):
        self.chunk_info = []
        self.size = 0

class File:

    def __init__(self, name, master, finfo):
        self.name = name
        self._master = master
        self._info = finfo
        self._chunks = [] # TODO: Only cache subset.
        self._offset = 0

    def _pull_chunk(self, cnum):
        cinfo = self._info.chunk_info[cnum]
        server = connect_chunk_server(cinfo.server_addr)
        return server.read_chunk(cinfo.id)

    # TODO: BUG CHECK
    def read(self, n):
        start_chunk = self._offset / CHUNK_SIZE
        start_off = self._offset % CHUNK_SIZE
        end_chunk = (self._offset + n) / CHUNK_SIZE
        end_off = (self._offset + n) % CHUNK_SIZE 

        if end_chunk >= len(self._info.chunk_info):
            end_chunk = len(self._info.chunk_info - 1)
            end_off = None

        for cnum in range(len(self._chunks), end_chunk + 1):
            chunk = self._pull_chunk(cnum)
            self._chunks.append(chunk)

        end_off = end_off or len(self._chunks[-1])

        if start_chunk is end_chunk:
            chunk = self._chunks[start_chunk]
            data = chunk[start_off:end_off]
            return data

        data = self._chunks[start_chunk][start_off:]
        for cnum in range(start_chunk + 1, end_chunk):
            data += self._chunks[cnum]
        data += self._chunks[end_chunk][:end_off]
        return data

    def write(self, data):
        n = 0
        dirty = []
        offset = self.size()
        
        while n < len(data):
            cnum = offset / CHUNK_SIZE
            if cnum == len(self._info.chunk_info):
                cinfo = self._master.request_chunk(self.name)
                self._info.chunk_info.append(cinfo)
                self._chunks.append(b"")
            if cnum == len(self._chunks):
                chunk = self._pull_chunk(cnum)
                self._chunks.append(chunk)
            chunk = self._chunks[cnum]
            rem = min(len(data) - n, CHUNK_SIZE - len(chunk))
            chunk += data[n:n+rem]
            n += rem
            offset += rem
            dirty.append(cnum)
            
        for cnum in dirty:
            chunk = self._chunks[cnum]
            info = self._info.chunk_info[cnum]
            server = connect_chunk_server(info.addr)
            server.write_chunk(
                id=info.id,
                offset=0,
                data=chunk
            )

        # TODO: Tell master about updated size.

        return n

    def seek(self, n):
        if n > self.size():
            raise ValueError("Cannot seek to n > file size.")
        self._offset = n

    def size(self):
        return self._info.size()

class Client:

    def __init__(self, master):
        self._master = master

    def create(self, fname):
        pass

    def open(self, fname):
        finfo = self._master.open(fname)
        f = File(fname, self._master, finfo)
        return f

    def stat(self, fname):
        finfo = self._master.stat(fname)
        return finfo

def connect_client(addr):
    m = connect_master(addr)
    c = Client(m)
    return c

def client_main():
    client = connect("127.0.0.1:8888")
    stats = client.stat("hello.txt")
    f = client.create("hello.txt")
    f.write("hello world!")
    f.write("this has been fun indeed")
    f.close()
    f = client.open("hello.txt")
    while True:
        data = f.read(1024)
        if not data:
            break
        print("READ: {}".format(data))
    f.close()
        

if __name__ == "__main__":
    chunk_server_main()
