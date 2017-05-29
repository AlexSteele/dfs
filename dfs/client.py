
import master
import chunkserver
from constants import CHUNK_SIZE

class _ChunkCache:

    def __init__(self):
        self._cnum = -1
        self._chunk = None

    def contains(self, cnum):
        return self._cnum is cnum

    def get(self, cnum):
        assert(cnum is self._cnum)
        return self._chunk

    def add(self, cnum, chunk):
        self._cnum = cnum
        self._chunk = chunk

class File:

    def __init__(self, name, master, finfo):
        self.name = name
        self._master = master
        self._info = finfo
        self._cache = _ChunkCache()
        self._roffset = 0

    def _get_chunk(self, cnum):
        if self._cache.contains(cnum):
            return self._cache.get(cnum)
        cinfo = self._info.chunk_info[cnum]
        server = chunkserver.connect(cinfo.addr)
        chunk = server.read_chunk(cinfo.id)
        self._cache.add(cnum, chunk)
        return chunk

    def read(self, n):
        cnum = self._roffset / CHUNK_SIZE
        if cnum >= len(self._info.chunk_info):
            return b''
        chunk = self._get_chunk(cnum)
        coff = self._roffset % CHUNK_SIZE
        rem = min(n, len(chunk) - coff)
        if rem is 0:
            return b''
        data = chunk[coff:coff+rem]
        n -= rem
        self._roffset += rem

        while n > 0:
            cnum = self._roffset / CHUNK_SIZE
            if cnum >= len(self._info.chunk_info):
                break
            chunk = self._get_chunk(cnum)
            rem = min(n, len(chunk))
            if rem is 0:
                break
            data += chunk[:rem]
            n -= rem
            self._roffset += rem

        return data

    def write(self, data):

        if len(self._info.chunk_info) > 0:
            last = self._get_chunk(len(self._info.chunk_info) - 1)
            if len(last) < CHUNK_SIZE:
                cinfo = self._info.chunk_info[-1]
                rem = min(CHUNK_SIZE - len(last), len(data))
                server = chunkserver.connect(cinfo.addr)
                server.write_chunk(
                    chunkid=cinfo.id,
                    data=last+data[:rem]
                )
                data = data[rem:]

        while len(data) > 0:
            cinfo = self._master.request_new_chunk(self.name)
            rem = min(len(data), CHUNK_SIZE)
            server = chunkserver.connect(cinfo.addr)
            server.write_chunk(
                chunkid=cinfo.id,
                data=data[:rem]
            )
            self._info.chunk_info.append(cinfo)
            data = data[rem:]

        return len(data)

    def close(self):
        self._master.close(self.name)

class Client:

    def __init__(self, master):
        self._master = master

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def create(self, fname):
        finfo = self._master.create(fname)
        f = File(fname, self._master, finfo)
        return f

    def delete(self, fname):
        self._master.delete(fname)

    def open(self, fname):
        finfo = self._master.open(fname)
        f = File(fname, self._master, finfo)
        return f

    def stat(self, fname):
        finfo = self._master.stat(fname)
        return finfo

    def ping(self):
        self._master.ping()

    def close(self):
        self._master.closeconn()

def connect(addr):
    m = master.connect(addr)
    c = Client(m)
    return c
