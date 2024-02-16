# this will be the client code
# ur a frickin nerd sam
import socket
import threading
import random
import pickle
import csv 

# Port range 18,000 to 18,499

MANAGER_IP = "localhost"
MANAGER_PORT = 9999
MY_IP = "localhost"
MY_PORT = random.randint(18000, 18499)
ENCODER = "utf-8"

i_am_leader = False
my_identifier = 0
MAX_UDP_SIZE = 65507
size_of_ring = 0

client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_socket.bind((MY_IP, MY_PORT))

# Debugging Purposes
print(MY_PORT)


def split_the_message(full_message):
    command_to_send = full_message.split(" ", 1)
    return command_to_send


# This Function takes in a year and opens the corresponding CSV file
# Returns a CSV File object
def open_storm_data(year):
    if year == 1951 or year == 1952 or year in range(1990, 1992):
        with open(('storm_data\details-' + str(year) + '.csv'), 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for entry in csv_reader:
                print(entry)


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
            pickle_data, incoming_addr = client_socket.recvfrom(MAX_UDP_SIZE)
            data = pickle.loads(pickle_data)
            print(data)

            global size_of_ring
            global my_identifier
            #print(i_am_leader)

            if i_am_leader:
                code, list_of_dht, year = data
                size_of_ring = len(list_of_dht)
                my_identifier = 0

                for i in range(1, size_of_ring):
                    #print(i)
                    set_id(list_of_dht[i], i, size_of_ring, list_of_dht)

                if year == 1951 or year == 1952 or year in range(1990, 1992):
                    with open(('storm_data\details-' + str(year) + '.csv'), 'r') as csv_file:
                        csv_reader = csv.reader(csv_file)
                        num_of_lines_in_csv = len(list(csv_reader)) - 1

                        next(csv_reader)    # This skips the first line of the file
                        for entry in csv_reader:
                            formatted_entry = tuple(entry)

                            # ---------- Distributing entries to peers --------------


            command1, id_data, size, neighbor = data
            #print(command1)

            if command1 == "set-id":
                my_identifier = id_data
                size_of_ring = size
                addr_of_right_neighbor = (my_identifier + 1) % size_of_ring
                right_neighbor = neighbor[addr_of_right_neighbor]
            
                # Debugging
                # print(my_identifier)
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
        i_am_leader = True

        # Debugging CSV file reader

        client_socket.sendto(f"{message}".encode(), (MANAGER_IP, MANAGER_PORT))
    else:
        client_socket.sendto(f"{message}".encode(), (MANAGER_IP, MANAGER_PORT))
