
class MemEnv:

    def __init__(self):
        self._files = {}
        self._ftable = []

    def open(self, fname, mode):
        if mode == "w+":
            f = {
                "name": fname,
                "buf": b"",
                "offset": 0
            }
            self._files[fname] = f
            self._ftable.append(f)
        else:
            f = self._files.get(fname, None)
            if not f:
                raise IOError("No such file {}".format(fname))
            self._ftable.append(f)
            
        return len(self._ftable) - 1

    def close(self, fd):
        f = self._ftable[fd]
        f["offset"] = 0
        self._ftable = self._ftable[:fd] + self._ftable[fd+1:]

    def remove(self, fname):
        # TODO: Concurrent access
        if fname not in self._files:
            raise IOError("{} does not exist.".format(fname))
        self._files.pop(fname, None)
        self._ftable = filter(lambda e: e["name"] != fname, self._ftable)

    def seek(self, fd, pos):
        f = self._ftable[fd]
        assert(pos <= len(f["buf"]))
        f["offset"] = pos
        
    def read(self, fd, bufsize):
        f = self._ftable[fd]
        n = min(bufsize, len(f["buf"]) - f["offset"])
        buf = f["buf"][f["offset"]:f["offset"] + n]
        f["offset"] += n
        return buf

    def readall(self, fd):
        buf = b""
        while True:
            b = self.read(fd, 4092)
            if not b:
                break
            buf += b
        return buf

    def readrange(self, fd, start_offset, end_offset):
        l = end_offset - start_offset
        buf = b""

        self.seek(fd, start_offset)
        while len(buf) < l:
            b = self.read(fd, l - len(buf))
            if not b:
                break
            buf += b
        return buf

    def write(self, fd, data):
        f = self._ftable[fd]
        buf = f["buf"]
        f["buf"] = buf[:f["offset"]] + data + buf[f["offset"]:]
        f["offset"] += len(data)
