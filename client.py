# this will be the client code
# ur a frickin nerd sam
import socket
import threading
import random

# Port range 18,000 to 18,499

DEST_IP = "localhost"
DEST_PORT = 9995
SOURCE_IP = "localhost"
SOURCE_PORT = random.randint(18000, 18499)
ENCODER = "utf-8"
SETUPBOOL = False
identifier = 0

client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_socket.bind((SOURCE_IP, SOURCE_PORT))


def split_the_message(full_message):
    command_to_send = full_message.split(" ", 1)
    return command_to_send


def set_id(peer_tuple_data, id_number, size, full_tuple_data):
    client_socket.sendto(f"{id_number, size}".encode(), (peer_tuple_data[1], peer_tuple_data[2])) # pickle the
    # full_tuple_data
    pass


def receive():
    while True:
        try:
            msg, _ = client_socket.recvfrom(1024)
            if SETUPBOOL:
                info = msg.decode # this is where pickling will happen
                size_of_ring = len(info)
                global identifier
                identifier = 0
                for i in range(size_of_ring):
                    set_id(info[i], i, size_of_ring, msg)

            else:
                print(msg.decode())

        except:
            pass


t = threading.Thread(target=receive)
t.start()

while True:
    message = input("")
    command, instruction = split_the_message(message)

    if command == "quit":
        exit()
    elif command == "setup-dht":
        client_socket.sendto(f"{message}".encode(), (DEST_IP, DEST_PORT))
        SETUPBOOL = True
    else:
        client_socket.sendto(f"{message}".encode(), (DEST_IP, DEST_PORT))
