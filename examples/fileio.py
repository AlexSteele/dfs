
import setup
import client
from constants import DEFAULT_MASTER_CLIENT_PORT

def main():

    cl = client.connect(("", DEFAULT_MASTER_CLIENT_PORT))

    f = cl.create("hello.txt")
    for i in range(10):
        f.write(b"Hello")
    f.close()

    f = cl.open("hello.txt")
    while True:
        buf = f.read(len("Hello"))
        if not buf:
            break
        print(buf)
    f.close()

    cl.delete("hello.txt")
    cl.close()

if __name__ == "__main__":
    main()
