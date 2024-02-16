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
identifier = 0
addr_of_right_neighbor = 0
right_neighbor = ("", 0)
MAX_UDP_SIZE = 65507
size_of_ring = 0

local_hash_table = {}

client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_socket.bind((MY_IP, MY_PORT))

# Debugging Purposes
print(MY_PORT)


def is_prime(num):
    if num < 2:
        return False
    for i in range(2, int(num ** 0.5) + 1):
        if num % i == 0:
            return False
    return True


def next_prime_after_2l(file_size):
    num = 2 * file_size + 1
    while True:
        if is_prime(num):
            return num
        num += 2


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
    # print((peer_tuple_data[1], peer_tuple_data[2]))

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
            global identifier
            global addr_of_right_neighbor
            global right_neighbor

            if i_am_leader:
                code, list_of_dht, year = data

                identifier = 0
                size_of_ring = len(list_of_dht)
                addr_of_right_neighbor = (identifier + 1) % size_of_ring
                right_neighbor = list_of_dht[addr_of_right_neighbor]

                for i in range(1, size_of_ring):
                    # print(i)
                    set_id(list_of_dht[i], i, size_of_ring, list_of_dht)

                if year == 1951 or year == 1952 or year in range(1990, 1992):
                    with open(('storm_data\details-' + str(year) + '.csv'), 'r') as csv_file:
                        csv_reader = csv.reader(csv_file)
                        num_of_lines_in_csv = len(list(csv_reader)) - 1
                        big_prime = next_prime_after_2l(num_of_lines_in_csv)

                        next(csv_reader)    # This skips the first line of the file
                        for entry in csv_reader:
                            formatted_entry = tuple(entry)

                            # ---------- Distributing entries to peers --------------
                            event_id = formatted_entry[0]
                            pos = event_id % big_prime
                            node_id = pos % size_of_ring

                            if node_id == identifier:
                                local_hash_table[pos].append(formatted_entry)
                            else:
                                packet = "store", node_id, pos, formatted_entry
                                serialized_packet = pickle.dumps(packet)
                                client_socket.sendto(serialized_packet, (right_neighbor[1], int(right_neighbor[2])))

                

                # a block here to check if the leader should store the record
                #
                # if node_id == identifier:
                #       local_hash_table[pos].append(current_entry)
                #
                #######
                #
                ## sending the record to the right neighbor

                

            # print(command1)

            if data[0] == "set-id" and not i_am_leader:
                command1, id_data, size, neighbor = data
                identifier = id_data
                size_of_ring = size
                addr_of_right_neighbor = (identifier + 1) % size_of_ring
                right_neighbor = neighbor[addr_of_right_neighbor]
            
                # Debugging
                # print(my_identifier)
                # print(size_of_ring)
                # print(addr_of_right_neighbor)
                # print(right_neighbor)

            if data[0] == "store" and not i_am_leader:
                # a block here to check if the peer should store the record
                command2, node_id, node_position, entry = data
                # event_id = entry[0]
                prime = next_prime_after_2l(event_id)
                my_pos = event_id % prime
                my_id = my_pos % size_of_ring
                if node_id == my_id:
                    local_hash_table[my_pos].append(entry)
                else:
                    packet = "store", my_id, node_position, entry
                    serialized_packet = pickle.dumps(packet)
                    client_socket.sendto(serialized_packet, (right_neighbor[1], int(right_neighbor[2])))
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
