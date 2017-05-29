
import setup
import unittest

from chunkserver import ChunkServer
import env

class TestChunkServer(unittest.TestCase):

    def test_create_chunk(self):
        cs = ChunkServer(env=env.MemEnv())
        cs.create_chunk("c1")
        cs.write_chunk("c1", b"12345")
        _ = cs.read_chunk("c1")

    def test_delete_chunk(self):
        cs = ChunkServer(env=env.MemEnv())
        cs.create_chunk("c1")
        cs.delete_chunk("c1")
        with self.assertRaises(IOError):
            _ = cs.read_chunk("c1")

    def test_delete_chunk_DNE(self):
        cs = ChunkServer(env=env.MemEnv())
        with self.assertRaises(IOError):
            cs.delete_chunk("c1")

    def test_read_write_chunk(self):
        cs = ChunkServer(env=env.MemEnv())
        cs.create_chunk("c1")
        msg = b"123456789"
        cs.write_chunk("c1", msg)
        res = cs.read_chunk("c1")
        self.assertEquals(msg, res)

    def test_read_chunk_range(self):
        cs = ChunkServer(env=env.MemEnv())
        cs.create_chunk("c1")
        msg = b"123456789"
        cs.write_chunk("c1", msg)
        res = cs.read_chunk(
            chunkid="c1",
            start_offset=1,
            end_offset=3
        )
        self.assertEquals(res, msg[1:3])

    def test_write_chunk_DNE(self):
        cs = ChunkServer(env=env.MemEnv())
        with self.assertRaises(IOError):
            cs.write_chunk("c1", b"12345")

if __name__ == "__main__":
    unittest.main()
