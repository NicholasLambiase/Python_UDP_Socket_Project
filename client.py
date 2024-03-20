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
right_neighbor = ("", "", 0)  # name , IPV4 address, port number
peer_to_query = ("", "", 0)  # name, IPV4 address, port number
big_prime = 0
id_seq = []
peer_list = []
list_of_peers_in_ring = []
leave_condition = False
join_condition = False
year = 0

# Setting up the Client Socket using UDP
# Port range 18,000 to 18,499
client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_socket.bind((MY_IP, MY_PORT))

# Debugging Purposes
print(MY_PORT)
register_message = "register", "client" + str(MY_PORT), MY_IP, str(MANAGER_PORT), str(MY_PORT)
print(register_message)
client_socket.sendto(pickle.dumps(register_message), (MANAGER_IP, MANAGER_PORT))


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


def set_id(target_peer_data, id_number, ring_size, full_peer_list, year):
    obj_to_send = "set-id", id_number, ring_size, full_peer_list, year
    serialized_obj = pickle.dumps(obj_to_send)
    client_socket.sendto(serialized_obj, (target_peer_data[1], int(target_peer_data[2])))


def receive():
    while True:
        try:
            received_serialized_message, incoming_addr = client_socket.recvfrom(MAX_UDP_SIZE)
            received_message = pickle.loads(received_serialized_message)
            peer_address, peer_port = incoming_addr
            

            global size_of_ring, node_entries_counter, my_identifier, id_of_right_neighbor, right_neighbor, \
                peer_to_query, big_prime, id_seq, peer_list, local_hash_table, leave_condition, list_of_peers_in_ring, \
                i_am_leader, year, join_condition, csv_file_entries


            # Finding who sent me a message
            who_i_received_from = ""
            if peer_port == MANAGER_PORT:
                who_i_received_from = "Manager"
            else:
                for peer in peer_list:
                    if peer[2] == peer_port:
                        who_i_received_from = peer[0]

            if received_message[0] != "store" and received_message[0] != "set-id":
                print(f"Client received '{received_message[0]}' from {who_i_received_from}")

            if received_message[0] == "dht-rebuilt":
                print("Sending 'dht-rebuilt' to the Manager")
                send = "dht-rebuilt", "filler"
                client_socket.sendto(pickle.dumps(send), (MANAGER_IP, MANAGER_PORT))

            if received_message[0] == "reset-id":
                command1, id_to_assign, received_ring_size, neighbors, return_ip, return_port, year = received_message

                if not leave_condition:
                    my_identifier = int(id_to_assign)
                    i_am_leader = False
                    if my_identifier == 0:
                        i_am_leader = True
                    else:
                        i_am_leader = False

                    size_of_ring = int(received_ring_size)

                    id_of_right_neighbor = (my_identifier + 1) % size_of_ring
                    print(f"This is the new ID of my Right Neighbor: {id_of_right_neighbor}")

                    right_neighbor = neighbors[id_of_right_neighbor]

                    peer_list = neighbors

                    id_to_assign = int(id_to_assign)
                    id_to_assign = id_to_assign + 1

                    print("this is my right neighbour: " + str(right_neighbor[2]))

                    if id_to_assign >= size_of_ring:
                        if not join_condition:
                            msg = "reset-id", id_to_assign, size_of_ring, peer_list, return_ip, return_port, year
                            client_socket.sendto(pickle.dumps(msg), (return_ip, int(return_port)))
                        else:
                            rebuild_command = "rebuild-dht", "SUCCESS", neighbors, year
                            client_socket.sendto(pickle.dumps(rebuild_command),
                                                 (right_neighbor[1], int(right_neighbor[2])))
                    else:
                        reset_message = "reset-id", id_to_assign, size_of_ring, peer_list, return_ip, return_port, year
                        client_socket.sendto(pickle.dumps(reset_message), (right_neighbor[1], int(right_neighbor[2])))
                else:
                    rebuild_command = "rebuild-dht", "SUCCESS", neighbors, year
                    client_socket.sendto(pickle.dumps(rebuild_command), (right_neighbor[1], int(right_neighbor[2])))

            if received_message[0] == "leave":
                # This means that I am the peer leaving the DHT
                leave_condition = True

                teardown_message = "teardown", "partial_leaving", received_message[1]
                serialized_packet = pickle.dumps(teardown_message)
                client_socket.sendto(serialized_packet, (right_neighbor[1], int(right_neighbor[2])))

            if received_message[0] == "join":
                join_condition = True
                print("this is the join condition: " + str(join_condition))

                peers = received_message[2]

                teardown_message = "teardown", "joining", received_message[1], peers
                serialized_packet = pickle.dumps(teardown_message)
                client_socket.sendto(serialized_packet, (peers[0][1], int(peers[0][2])))

            if received_message[0] == "teardown-dht" and i_am_leader:
                teardown_message = "teardown", "complete"

                serialized_packet = pickle.dumps(teardown_message)

                client_socket.sendto(serialized_packet, (right_neighbor[1], int(right_neighbor[2])))

            if received_message[0] == "teardown":
                if received_message[1] == "complete":
                    local_hash_table = {}
                    if not i_am_leader:
                        serialized_packet = pickle.dumps(received_message)
                        client_socket.sendto(serialized_packet, (right_neighbor[1], int(right_neighbor[2])))
                    else:
                        packet = "teardown-complete", "SUCCESS"
                        serialized_packet = pickle.dumps(packet)
                        client_socket.sendto(serialized_packet, (MANAGER_IP, MANAGER_PORT))

                if received_message[1] == "partial_leaving":
                    local_hash_table = {}
                    if not leave_condition:     # If I am in the DHT and not leaving
                        serialized_packet = pickle.dumps(received_message)
                        client_socket.sendto(serialized_packet, (right_neighbor[1], int(right_neighbor[2])))
                    else:   # I am the client that is leaving 
                        # message = ("teardown", "partial-leaving", "client_name_leaving") AND leave-conditions is TRUE
                        size_of_new_ring = (size_of_ring - 1)

                        # Remove the Peer that is leaving from the peer_list
                        for peer in peer_list:
                            if peer[0] == received_message[2]:
                                leaving_peer = (peer[0], peer[1], peer[2])
                                peer_list.remove(leaving_peer)
                        
                        print(peer_list)
                        reset_message = "reset-id", 0, size_of_new_ring, peer_list, MY_IP, MY_PORT, year
                        client_socket.sendto(pickle.dumps(reset_message), (right_neighbor[1], int(right_neighbor[2])))

                if received_message[1] == "joining":
                    if local_hash_table:
                        local_hash_table = {}
                        print("Got the message")
                        serialized_packet = pickle.dumps(received_message)
                        client_socket.sendto(serialized_packet, (right_neighbor[1], int(right_neighbor[2])))
                    else:
                        print("back to leader")
                        joining_peer = (received_message[2][0], received_message[2][1], received_message[2][2])
                        list_of_peers_in_ring.append(joining_peer)
                        print(list_of_peers_in_ring)
                        size_of_new_ring = (size_of_ring + 1)
                        my_identifier = 0
                        i_am_leader = True
                        size_of_ring = size_of_new_ring
                        id_of_right_neighbor = (my_identifier + 1) % size_of_ring
                        right_neighbor = list_of_peers_in_ring[id_of_right_neighbor]

                        peer_list = list_of_peers_in_ring

                        id_to_assign = int(my_identifier)
                        id_to_assign = id_to_assign + 1

                        reset_message = "reset-id", id_to_assign, size_of_new_ring, peer_list, MY_IP, MY_PORT, year
                        client_socket.sendto(pickle.dumps(reset_message), (right_neighbor[1], int(right_neighbor[2])))

            if received_message[0] == "deregister":
                code = received_message[1]
                if code == "SUCCESS":
                    print(code)
                    quit(0)

            if received_message[0] == "query":
                # print(received_message[1])
                peer_to_query = received_message[2]
                big_prime = received_message[3]
                # print(peer_to_query)

            if received_message[0][0] == "find-event":

                prime = received_message[2]
                event_id = received_message[0][1]
                print("this is event id: " + str(event_id))
                original_add = received_message[1]
                id_seq = received_message[3]
                id_seq.append(my_identifier)

                query_pos = (int(event_id)) % (int(prime))
                print(query_pos)
                query_id = query_pos % size_of_ring
                print(query_id)
                if query_id == my_identifier:
                    try:
                        value_to_compare = local_hash_table[query_pos]

                        if value_to_compare[0][0] == event_id:
                            query_message = "SUCCESS", local_hash_table[query_pos], id_seq
                            client_socket.sendto(pickle.dumps(query_message), (original_add[0], int(original_add[1])))
                    except KeyError:
                        fail_message = "FAILURE", "Storm event " + str(event_id) + " not found in the DHT"
                        client_socket.sendto(pickle.dumps(fail_message), (original_add[0], int(original_add[1])))
                else:
                    eye_list = []
                    eye = list(range(0, size_of_ring))

                    for integer in eye:
                        if integer not in id_seq:
                            eye_list.append(integer)

                    print(eye_list)

                    next_peer_id = random.choice(eye_list)
                    print(next_peer_id)
                    # event_msg = mssg_list, peer_to_query, big_prime
                    query_message_p = "find-event", event_id
                    next_query_node = peer_list[next_peer_id]
                    next_query_message = query_message_p, original_add, prime, id_seq
                    client_socket.sendto(pickle.dumps(next_query_message),
                                         (next_query_node[1], int(next_query_node[2])))

            if (received_message[0] == "setup-dht" or received_message[0] == "rebuild-dht") and i_am_leader:
                message1, response_code, list_of_peers_in_ring, year = received_message
                print("in setup")
                print(list_of_peers_in_ring)
                print(size_of_ring)
                year = int(year)

                # Setting up leader identification / Global Peer Variables
                my_identifier = 0
                size_of_ring = len(list_of_peers_in_ring)
                id_of_right_neighbor = (my_identifier + 1) % size_of_ring
                right_neighbor = list_of_peers_in_ring[id_of_right_neighbor]
                peer_list = list_of_peers_in_ring

                # DEBUG Checking that Peers get Registered
                # print(f"My ID: {my_identifier}")
                # print(f"ID of Right Neighbor: {id_of_right_neighbor}")

                if year in range(1949, 1953) or year in range(1989, 1997):
                    with open(('storm_data\details-' + str(year) + '.csv'), 'r') as csv_file:
                        csv_reader = csv.reader(csv_file)

                        # This will copy each line in the CSV file as Tuples into a local list
                        next(csv_reader)  # This skips the first line of the CSV File
                        for line in csv_reader:
                            # print(line)
                            csv_file_entries.append(tuple(line))

                        # Extracting neccesary data from our local copy of the CSV file
                        num_of_lines_in_csv = len(csv_file_entries)
                        big_prime = next_prime_after_2l(num_of_lines_in_csv)

                        # Setting the IDs of each user in the DHT Ring
                        for entry_id in range(1, size_of_ring):
                            set_id(list_of_peers_in_ring[entry_id], entry_id, size_of_ring, list_of_peers_in_ring, year)

                        # This will initialize a counter for each node in the DHT to the node_entries_counter
                        for entry_id in range(0, size_of_ring):
                            node_entries_counter.insert(entry_id, 0)

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

                    node_entries_counter = []
                    csv_file_entries = []

                    # Send the dht-complete code to the manager to allow it to accept incoming requests
                    if message1 == "setup-dht":
                        dht_complete_message = "dht-complete", list_of_peers_in_ring[0][0], big_prime
                        client_socket.sendto(pickle.dumps(dht_complete_message), (MANAGER_IP, MANAGER_PORT))
                    elif message1 == "rebuild-dht":
                        dht_complete_message = "dht-rebuilt", list_of_peers_in_ring[0][0], big_prime
                        client_socket.sendto(pickle.dumps(dht_complete_message), (peer_address, int(peer_port)))

            elif received_message[0] == "set-id" and not i_am_leader:
                command1, id_to_assign, received_ring_size, neighbors, year = received_message
                my_identifier = int(id_to_assign)
                size_of_ring = int(received_ring_size)
                id_of_right_neighbor = (my_identifier + 1) % size_of_ring
                right_neighbor = neighbors[id_of_right_neighbor]
                peer_list = neighbors

                print(f"Client received '{received_message[0]}' from {peer_list[0][0]}")

                # DEBUG Checking that Peers get Registered
                # print(f"My ID: {my_identifier}")
                # print(f"ID of Right Neighbor: {id_of_right_neighbor}")

            elif received_message[0] == "store" and not i_am_leader:
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
        print("bye bye")
        exit(1)
    elif mssg_list[0] == "register":
        client_socket.sendto(pickle.dumps(mssg_list), (MANAGER_IP, MANAGER_PORT))
    elif mssg_list[0] == "setup-dht":
        i_am_leader = True
        client_socket.sendto(pickle.dumps(mssg_list), (MANAGER_IP, MANAGER_PORT))
    elif mssg_list[0] == "query-dht":
        client_socket.sendto(pickle.dumps(mssg_list), (MANAGER_IP, MANAGER_PORT))
    elif mssg_list[0] == "find-event":
        my_info = MY_IP, MY_PORT
        event_msg = mssg_list, my_info, big_prime, id_seq
        client_socket.sendto(pickle.dumps(event_msg), (peer_to_query[1], int(peer_to_query[2])))
    elif mssg_list[0] == "teardown-dht":
        client_socket.sendto(pickle.dumps(mssg_list), (MANAGER_IP, MANAGER_PORT))
    elif mssg_list[0] == "deregister":
        client_socket.sendto(pickle.dumps(mssg_list), (MANAGER_IP, MANAGER_PORT))
        quit(0)
    elif mssg_list[0] == "leave-dht":
        mssg_list.append(str(right_neighbor[0]))
        client_socket.sendto(pickle.dumps(mssg_list), (MANAGER_IP, MANAGER_PORT))
    elif mssg_list[0] == "join-dht":
        client_socket.sendto(pickle.dumps(mssg_list), (MANAGER_IP, MANAGER_PORT))

    elif message == "print table":
        for key, value in local_hash_table.items():
            print(key, ":", value)
    # shortcuts
    elif message == "s":
        i_am_leader = True
        mssg_list = "setup-dht", "client" + str(MY_PORT), "5", "1996"
        client_socket.sendto(pickle.dumps(mssg_list), (MANAGER_IP, MANAGER_PORT))
    elif message == "q":
        mssg_list = "query-dht", "client" + str(MY_PORT)
        client_socket.sendto(pickle.dumps(mssg_list), (MANAGER_IP, MANAGER_PORT))
    elif message == "p":
        print("My Peer Name is: client" + str(MY_PORT) + "\n")
        for peers in peer_list:
            print(peers[0])

    else:
        print("Please enter a valid command")
