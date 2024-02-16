# this will be the client code
# ur a frickin nerd sam
import socket
import threading
import random
import pickle

# Port range 18,000 to 18,499

DEST_IP = "localhost"
DEST_PORT = 9999
SOURCE_IP = "localhost"
SOURCE_PORT = random.randint(18000, 18499)
ENCODER = "utf-8"

SETUPBOOL = False
identifier = 0
MAX_UDP_SIZE = 65507
size_of_ring = 0

client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_socket.bind((SOURCE_IP, SOURCE_PORT))

print(SOURCE_PORT)


def split_the_message(full_message):
    command_to_send = full_message.split(" ", 1)
    return command_to_send


def set_id(peer_tuple_data, id_number, size, full_tuple_data):
    #print((peer_tuple_data[1], peer_tuple_data[2]))

    obj_to_send = "set-id", id_number, size, full_tuple_data
    serialized_obj = pickle.dumps(obj_to_send)
    client_socket.sendto(serialized_obj, (peer_tuple_data[1], int(peer_tuple_data[2])))  # pickle the
    # full_tuple_data
    pass


def receive():
    while True:
        try:
            pickle_data, _ = client_socket.recvfrom(MAX_UDP_SIZE)
            data = pickle.loads(pickle_data)
            print(data)

            global size_of_ring
            global identifier
            #print(SETUPBOOL)

            if SETUPBOOL:
                code, list_of_dht, year = data

                size_of_ring = len(list_of_dht)

                identifier = 0

                for i in range(1, size_of_ring):
                    #print(i)
                    set_id(list_of_dht[i], i, size_of_ring, list_of_dht)

            command1, id_data, size, neighbor = data
            #print(command1)

            if command1 == "set-id":
                identifier = id_data
                size_of_ring = size
                addr_of_right_neighbor = (identifier + 1) % size_of_ring
                right_neighbor = neighbor[addr_of_right_neighbor]
                # print(identifier)
                # print(size_of_ring)
                # print(addr_of_right_neighbor)
                # print(right_neighbor)
            else:
                # print(data)
                pass
        except:
            pass


t = threading.Thread(target=receive)
t.start()

while True:
    message = input("")
    command, instruction = split_the_message(message)

    if command == "quit":
        exit(0)
    elif command == "setup-dht":
        client_socket.sendto(f"{message}".encode(), (DEST_IP, DEST_PORT))
        SETUPBOOL = True
    else:
        client_socket.sendto(f"{message}".encode(), (DEST_IP, DEST_PORT))
