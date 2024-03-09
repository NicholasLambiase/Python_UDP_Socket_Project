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
dht_setup_status = False
peer_count = 0

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
    peers_in_dht = []

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

    return "setup-dht", "SUCCESS", peers_in_dht, year  # return SUCCESS, the list of dht peers, and year


def receive():
    while True:
        try:
            message, addr = manager_socket.recvfrom(1024)
            messages.put((message, addr))
        except:
            pass


def broadcast():
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
            elif command == "teardown-dht":
                leader_found = False
                for peer in list_of_peers:
                    if peer["name"] == message[1] and peer["status"] == Leader:
                        leader_found = True
                if not leader_found:
                    print("FAILURE")
                else:
                    serialized_message = pickle.dumps("teardown-dht")
                    manager_socket.sendto(serialized_message, (peer_address, int(peer_port)))
            elif command == "teardown-complete":
                print("SUCCESS")
            else:
                manager_socket.sendto("invalid command".encode(), (peer_address, peer_port))


t1 = threading.Thread(target=receive)
t2 = threading.Thread(target=broadcast)

t1.start()
t2.start()
