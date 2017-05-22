
import json
import struct

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

def call_sync(conn, method, **args):
    args["method"] = method
    sendmsg(conn, args)
    resp = recvmsg(conn)
    return resp
