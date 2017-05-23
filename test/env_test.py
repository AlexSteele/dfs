
import setup
import unittest

import env

class TestMemEnv(unittest.TestCase):

    def test_open_new(self):
        e = env.MemEnv()
        fd = e.open("a.txt", "w+")

    def test_open_non_existent(self):
        e = env.MemEnv()
        with self.assertRaises(IOError):
            e.open("a.txt", "r")

    def test_open_close(self):
        e = env.MemEnv()
        fd = e.open("a.txt", "w+")
        e.close(fd)
        fd = e.open("a.txt", "r")
        e.close(fd)

    def test_remove(self):
        e = env.MemEnv()
        fd = e.open("a.txt", "w+")
        e.close(fd)
        fd = e.open("a.txt", "r")
        e.close(fd)
        e.remove("a.txt")
        with self.assertRaises(IOError):
            _ = e.open("a.txt", "r")

    def test_seek(self):
        e = env.MemEnv()
        fd = e.open("a.txt", "w+")
        msg = "abcdefg"
        e.write(fd, msg)
        e.close(fd)
        fd = e.open("a.txt", "r")
        e.seek(fd, 2)
        buf = e.read(fd, 1)
        self.assertEquals(buf, msg[2])

    def test_read_write(self):
        e = env.MemEnv()
        fd = e.open("a.txt", "w+")
        msg = "abcdefg"
        e.write(fd, msg)
        e.close(fd)
        fd = e.open("a.txt", "r")
        buf = e.read(fd, len(msg))
        self.assertEquals(buf, msg)
        e.close(fd)

    def test_read_chunks(self):
        e = env.MemEnv()
        fd = e.open("a.txt", "w+")
        msg = "abcdefg"
        e.write(fd, msg)
        e.close(fd)
        fd = e.open("a.txt", "r")
        buf = "".join([e.read(fd, 1) for _ in range(len(msg))])
        self.assertEquals(buf, msg)

    def test_readall(self):
        e = env.MemEnv()
        fd = e.open("a.txt", "w+")
        msg = "abcdefg"
        e.write(fd, msg)
        e.close(fd)
        fd = e.open("a.txt", "r")
        buf = e.readall(fd)
        self.assertEquals(buf, msg)

    def test_readrange(self):
        e = env.MemEnv()
        fd = e.open("a.txt", "w+")
        msg = "abcdefg"
        e.write(fd, msg)
        e.close(fd)
        fd = e.open("a.txt", "r")
        buf = e.readrange(fd, start_offset=1, end_offset=2)
        self.assertEquals(buf, msg[1:2])

    def test_readrange_moves_offset(self):
        e = env.MemEnv()
        fd = e.open("a.txt", "w+")
        msg = "abcdefg"
        e.write(fd, msg)
        e.close(fd)
        fd = e.open("a.txt", "r")
        _ = e.readrange(fd, start_offset=1, end_offset=2)
        buf = e.read(fd, 1)
        self.assertEquals(buf, msg[2])

if __name__ == "__main__":
    unittest.main()
