
import setup
import unittest

from chunk_server import ChunkServer
from env import MemEnv
from master import Master, RoundRobinIter

def init_master():
    itr = RoundRobinIter()
    cservers = [
        ChunkServer(env=MemEnv()),
        ChunkServer(env=MemEnv()),
        ChunkServer(env=MemEnv())
    ]
    m = Master(
        chunk_server_iter=itr,
        chunk_servers=cservers
    )
    return m

class TestMaster(unittest.TestCase):

    def test_create(self):
        m = init_master()
        finfo = m.create("a.txt")
        self.assertEquals(len(finfo.chunk_info), 0)

if __name__ == "__main__":
    unittest.main()
