import socket
import threading
import random
import pickle
import csv

# Global Constants
MANAGER_IP = "localhost"
MANAGER_PORT = 9999
MY_IP = "localhost"
MY_PORT = random.randint(18000, 18499)
ENCODER = "utf-8"
MAX_UDP_SIZE = 65507

# Global Variables
local_hash_table = {}
csv_file_entries = []
i_am_leader = False
my_identifier = 0
size_of_ring = 0
node_entries_counter = []
id_of_right_neighbor = 0
right_neighbor = ("", "", 0)    # name , IPV4 address, port number

# Setting up the Client Socket using UDP
# Port range 18,000 to 18,499
client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_socket.bind((MY_IP, MY_PORT))

# Debugging Purposes
print(MY_PORT)

def split_the_message(full_message):
    parsed_mssg_list = full_message.split(" ")
    return parsed_mssg_list


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


def set_id(target_peer_data, id_number, ring_size, full_peer_list):
    obj_to_send = "set-id", id_number, ring_size, full_peer_list
    serialized_obj = pickle.dumps(obj_to_send)
    client_socket.sendto(serialized_obj, (target_peer_data[1], int(target_peer_data[2])))


def receive():
    while True:
        try:
            received_serialized_message, incoming_addr = client_socket.recvfrom(MAX_UDP_SIZE)
            received_message = pickle.loads(received_serialized_message)
            # print(received_message)

            global size_of_ring
            global node_entries_counter
            global my_identifier
            global id_of_right_neighbor
            global right_neighbor

            if i_am_leader:
                response_code, list_of_peers_in_ring, year = received_message
                year = int(year)

                # Setting up leader identification / Global Peer Variables
                my_identifier = 0
                size_of_ring = len(list_of_peers_in_ring)
                id_of_right_neighbor = (my_identifier + 1) % size_of_ring
                right_neighbor = list_of_peers_in_ring[id_of_right_neighbor]

                # DEBUG Checking that Peers get Registered
                # print(f"My ID: {my_identifier}")
                # print(f"ID of Right Neighbor: {id_of_right_neighbor}")

                if year in range(1950, 1952) or year in range(1990, 1992):
                    with open(('storm_data\details-' + str(year) + '.csv'), 'r') as csv_file:
                        csv_reader = csv.reader(csv_file)

                        # This will copy each line in the CSV file as Tuples into a local list
                        next(csv_reader)    # This skips the first line of the CSV File
                        for line in csv_reader:
                            # print(line)
                            csv_file_entries.append(tuple(line))

                        # Extracting neccesary data from our local copy of the CSV file
                        num_of_lines_in_csv = len(csv_file_entries)
                        big_prime = next_prime_after_2l(num_of_lines_in_csv)

                        # Setting the IDs of each user in the DHT Ring
                        for id in range(1, size_of_ring):
                            set_id(list_of_peers_in_ring[id], id, size_of_ring, list_of_peers_in_ring)

                        # This will initialize a counter for each node in the DHT to the node_entries_counter
                        for id in range(0, size_of_ring):
                            node_entries_counter.insert(id, 0)

                        # This copies the event ID number from the first index of the current entry tuple
                        for entry in csv_file_entries:
                            event_id = entry[0]         

                            # Determining Hash Values
                            pos = int(event_id) % big_prime
                            node_id = pos % size_of_ring

                            # Increments the count of the node choosen to store the entry
                            node_entries_counter[node_id] += 1

                            # If the leader is chosen from the hash above store the entry in the local hash
                            # Else send the entry to the appropriate ID using the Ring Structure
                            if node_id == my_identifier:
                                local_hash_table[int(pos)] = []
                                local_hash_table[int(pos)].append(entry)
                            else:
                                packet = "store", node_id, pos, entry
                                serialized_packet = pickle.dumps(packet)
                                client_socket.sendto(serialized_packet, (right_neighbor[1], int(right_neighbor[2])))
                        
                # Now that all the data has been sent to each of the peers in the DHT
                # We will print out the number of entries per peer to the console and send the DHT
                for index in range(0, len(node_entries_counter)):
                    print(f"Peer {index} has {node_entries_counter[index]} entries")

                # Send the dht-complete code to the manager to allow the it to accept incoming requests
                dht_complete_message = "dht-complete", list_of_peers_in_ring[0][0]
                client_socket.sendto(pickle.dumps(dht_complete_message), (MANAGER_IP, MANAGER_PORT))

            if received_message[0] == "set-id" and not i_am_leader:
                command1, id_to_assign, received_ring_size, neighbor = received_message
                my_identifier = int(id_to_assign)
                size_of_ring = int(received_ring_size)
                id_of_right_neighbor = (my_identifier + 1) % size_of_ring
                right_neighbor = neighbor[id_of_right_neighbor]

                # DEBUG Checking that Peers get Registered
                # print(f"My ID: {my_identifier}")
                # print(f"ID of Right Neighbor: {id_of_right_neighbor}")

            if received_message[0] == "store" and not i_am_leader:
                command2, node_id, node_position, entry = received_message
                node_id = int(node_id)
                node_position = int(node_position)

                if my_identifier == node_id:
                    local_hash_table[node_position] = []
                    local_hash_table[node_position].append(entry)

                    # DEBUG Printing the entry received from a peer that was just stored
                    # print(entry)

                else:
                    packet = "store", node_id, node_position, entry
                    serialized_packet = pickle.dumps(packet)
                    client_socket.sendto(serialized_packet, (right_neighbor[1], int(right_neighbor[2])))
            else:
                pass
        except:
            pass


t = threading.Thread(target=receive)
t.start()

while True:
    message = input("")
    mssg_list = split_the_message(message)

    # DEBUG mssg_list[0] = command and the list that is being sent
    print(mssg_list)

    if message == "quit":
        exit(0)
    elif mssg_list[0] == "register":
        client_socket.sendto(pickle.dumps(mssg_list), (MANAGER_IP, MANAGER_PORT))
    elif mssg_list[0] == "setup-dht":
        i_am_leader = True
        client_socket.sendto(pickle.dumps(mssg_list), (MANAGER_IP, MANAGER_PORT))
    elif message == "print table":
        for key, value in local_hash_table.items():
            print(key, ":", value)
    else:
        print("Please enter a valid command")