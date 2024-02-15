# this will be the client code
# ur a frickin nerd sam
import socket
import threading
import random

# Port range 18,000 to 18,499

DEST_IP = "localhost"
DEST_PORT = 9996
SOURCE_IP = "localhost"
SOURCE_PORT = random.randint(18000, 18499)
ENCODER = "utf-8"

client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_socket.bind((SOURCE_IP, SOURCE_PORT))



def receive():
    while True:
        try:
            msg, _ = client_socket.recvfrom(1024)
            print(msg.decode())
        except:
            pass


t = threading.Thread(target=receive)
t.start()



while True:
    message = input("")
    if message == "quit":
        exit()
    else:
        client_socket.sendto(f"{message}".encode(), (DEST_IP, DEST_PORT))


# if message.decode().startswith("registered:"):
                    #     name = message.decode()[message.decode().index(":")+1:]
                    #     print(name)
                    #
                    #     manager_socket.sendto(f"{name} joined".encode(), user)
                    # else:
                    #     #manager_socket.sendto(message, user)
                    #     pass