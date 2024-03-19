import socket
import threading
import queue
import random
import pickle

# Defining Constants;
Free = "free"
Leader = "leader"
InDht = "indht"
HOST_IP = "localhost"
HOST_PORT = 9999
ENCODER = "utf-8"
PEER_NAME_SIZE = 15

# Global
messages = queue.Queue()
list_of_peers = []
peers_in_dht = []
dht_setup_status = False
peer_count = 0
big_prime = 0
leaving_peer = ""
joining_peer = ""
peer_to_join = ()
# Setting Up the Socket
# Port range 18,000 to 18,499
manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
manager_socket.bind((HOST_IP, HOST_PORT))


def register(peer_name, ipv4_addr, m_port, p_port):
    # Create new peer Dict element
    new_peer = {"name": peer_name, "ipv4_addr": ipv4_addr, "m_port": m_port, "p_port": p_port, "status": Free}

    # Check to see if the peer already exists
    for existing_peer in list_of_peers:
        if existing_peer["name"] == new_peer["name"] or existing_peer["p_port"] == new_peer["p_port"]:
            return "FAILURE"

    # Since Peer does not yet exists append th new peer global list_of_peers
    list_of_peers.append(new_peer)
    global peer_count
    peer_count += 1
    return "SUCCESS"


def setup_dht(peer_name, number_of_peers, year):  # setup-dht logic
    global dht_setup_status
    global peers_in_dht

    # Minimum Conditions for a DHT to be set up
    if int(number_of_peers) < 3 or peer_count < int(number_of_peers) or dht_setup_status:
        return "FAILURE"

    # Find the peer that requested the setup_dht and make them the Leader
    # Append the Leader entry to the peers_in_dht as the first index of the list
    found_leader = False
    for existing_peers in list_of_peers:
        found_leader = peer_name in existing_peers["name"]
        if found_leader:
            existing_peers["status"] = Leader
            new_leader = (existing_peers["name"], existing_peers["ipv4_addr"], existing_peers["p_port"])
            peers_in_dht.append(new_leader)
            break

    if not found_leader:
        return "FAILURE"

    # Add peers at random from the list of registered peers to the DHT until the number of nodes needed is met
    while len(peers_in_dht) < int(number_of_peers):
        possible_peer = list_of_peers[random.randint(0, peer_count - 1)]

        if possible_peer["status"] == Free:
            possible_peer["status"] = InDht  # changing each peers status
            new_peer = (possible_peer["name"], possible_peer["ipv4_addr"], possible_peer["p_port"])
            peers_in_dht.append(new_peer)

    dht_setup_status = True
    return "setup-dht", "SUCCESS", peers_in_dht, year  # return SUCCESS, the list of dht peers, and year


def receive():
    while True:
        try:
            message, addr = manager_socket.recvfrom(1024)
            messages.put((message, addr))
        except:
            pass


def broadcast():
    global big_prime, peer_to_join
    global leaving_peer
    global joining_peer
    while True:
        while not messages.empty():
            # We will Parse the message we received to determine the command sent by the peer
            # The first parsed string of the message sent by the peer will be the command
            # All following parsed strings are parameters that will be passed to the command

            serialized_message, addr = messages.get()
            peer_address, peer_port = addr

            # Unpickle the data received. The format will be a list with index 0 as the command 
            # all following elements are command parameters
            message = pickle.loads(serialized_message)
            command = message[0]

            # DEBUG Checking to see if the pickled message was received
            print(f"Manager has entered '{command}'")

            if command == "register":
                result_message = register(message[1], message[2], message[3],
                                          message[4])
                print(result_message)
                pickled_message = pickle.dumps(result_message)
                manager_socket.sendto(pickled_message, (peer_address, peer_port))

            elif command == "setup-dht":
                setup_results = setup_dht(message[1], message[2], message[3])
                serialized_result = pickle.dumps(setup_results)
                manager_socket.sendto(serialized_result, (peer_address, peer_port))

            elif command == "dht-complete":
                for peer in list_of_peers:
                    if peer["status"] == Leader and peer["name"] == message[1]:
                        print("dht-complete")
                big_prime = message[2]

            elif command == "query-dht":
                if not dht_setup_status:
                    print("FAILURE")
                else:
                    peer_to_query = message[1]
                    print(peer_to_query)
                    if_peer_is_in = False
                    for peer in list_of_peers:

                        if peer["name"] == peer_to_query and (peer["status"] == Free):
                            if_peer_is_in = True
                    if not if_peer_is_in:
                        print("FAILURE")
                    else:
                        query_peer = peers_in_dht[random.randint(0, len(peers_in_dht) - 1)]
                        print(query_peer)
                        msg_to_send = "query", "SUCCESS", query_peer, big_prime
                        manager_socket.sendto(pickle.dumps(msg_to_send), (peer_address, peer_port))

            elif command == "leave-dht":

                peer_to_leave = message[1]
                new_leader = message[2]
                
                if_peer_is_in = False
                for peer in list_of_peers:
                    if peer["name"] == peer_to_leave and (peer["status"] == InDht or peer["status"] == Leader):
                        in_dht_entry_to_remove = (peer["name"], peer["ipv4_addr"], peer["p_port"])
                        if_peer_is_in = True

                if not dht_setup_status or not if_peer_is_in:
                    print("FAILURE")

                leaving_peer = peer_to_leave
                leave_msg = "leave", leaving_peer, "SUCCESS"
                manager_socket.sendto(pickle.dumps(leave_msg), (peer_address, peer_port))

                # Remove the peer leaving from peers_in_dht
                peers_in_dht.remove(in_dht_entry_to_remove)

                # Change the leaving Peer's Status to Free and update the new leader status of the new leader and the old leader
                for peers in list_of_peers:
                    print(f"Peer being checked: {peers}")
                    if peers["name"] == peer_to_leave:
                        print("Peer leaving found. Status set to free\n\n")
                        peers["status"] = Free
                    
                    if peers["name"] == new_leader:
                        print("New Leader Found. Status set to Leader\n\n")
                        peers["status"] = Leader
                    
                    if peers["status"] == Leader and peers["name"] != new_leader and peers["name"] != peer_to_leave:
                        print("Updating the Status of the old leader to InDHT.\n\n")
                        peers["status"] = InDht

            elif command == "join-dht":
                if not dht_setup_status:
                    print("FAILURE")
                for peer in list_of_peers:
                    if peer["name"] == message[1] and peer["status"] != Free:
                        print("FAILURE")
                for peer in list_of_peers:
                    if peer["name"] == message[1]:
                        peer_to_join = (peer["name"], peer["ipv4_addr"], peer["p_port"])

                joining_peer = message[1]
                msg = "join", peer_to_join, peers_in_dht, "SUCCESS"
                manager_socket.sendto(pickle.dumps(msg), (peer_address, peer_port))

                # Append the newly joining peer to peers_in_dht
                peers_in_dht.append(peer_to_join)

                # Change the joining peer's status to InDHT
                for peers in list_of_peers:
                    print(f"Peer being checked: {peers}")
                    if peers["name"] == peer_to_join[0]:
                        print("Found the joining peer. Updating his status to InDHT\n\n")
                        peers["status"] = InDht

            elif command == "dht-rebuilt":
                print("SUCCESS")

            elif command == "deregister":

                peer_to_deregister = message[1]
                print(peer_to_deregister)
                if_peer_is_in = False
                for peer in list_of_peers:
                    if peer["name"] == peer_to_deregister and (peer["status"] == InDht or peer["status"] == Leader):
                        if_peer_is_in = True
                if if_peer_is_in:
                    print("FAILURE")
                else:
                    for peer in list_of_peers:
                        if peer["name"] == peer_to_deregister:
                            rip_peer = {"name": peer["name"], "ipv4_addr": peer["ipv4_addr"], "m_port": peer["m_port"],
                                        "p_port": peer["p_port"],
                                        "status": peer["status"]}
                            list_of_peers.remove(rip_peer)
                            msg_to_send = "deregister", "SUCCESS"
                            manager_socket.sendto(pickle.dumps(msg_to_send), (peer_address, peer_port))

            elif command == "teardown-dht":
                leader_found = False
                for peer in list_of_peers:
                    if peer["name"] == message[1] and peer["status"] == Leader:
                        leader_found = True
                if not leader_found:
                    print("FAILURE")
                else:
                    msg = "teardown-dht", "SUCCESS"
                    serialized_message = pickle.dumps(msg)
                    manager_socket.sendto(serialized_message, (peer_address, int(peer_port)))

            elif command == "teardown-complete":
                print("SUCCESS")

            elif command == "quit":
                exit(0)

            else:
                manager_socket.sendto("invalid command".encode(), (peer_address, peer_port))


t1 = threading.Thread(target=receive)
t2 = threading.Thread(target=broadcast)

t1.start()
t2.start()

while True:
    message = input("")

    if message == "p":
        print("\nlist_of_peers:")
        for peers in list_of_peers:
            if peers["status"] == InDht:
                print(f"InDHT:\t{peers}")
            elif peers["status"] == Free:
                print(f"Free:\t{peers}")
            else:
                print(f"Leader:\t{peers}")

        print("\npeers_in_dht:")
        for peers in peers_in_dht:
            print(peers[0])
