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
in_dht_entry_to_remove = ()
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
    global big_prime, peer_to_join, in_dht_entry_to_remove
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
                print(f"Received the register Command from: {message[1]}")
                result_message = register(message[1], message[2], message[3],
                                          message[4])
                
                #Print the result of the register function
                if result_message == "SUCCESS":
                    print(f"Registered {message[1]} as a Free peer")
                    print("SUCCESS\n\n")
                else:
                    print(f"{message[1]} was not registered")
                    print("FAILURE\n\n")

                # Send the result message back to the peer
                pickled_message = pickle.dumps(result_message)
                manager_socket.sendto(pickled_message, (peer_address, peer_port))

            elif command == "setup-dht":
                setup_results = setup_dht(message[1], message[2], message[3])

                #Print the result of setup-dht
                if setup_results[0] == "setup-dht":
                    print(f"Setting up a DHT of size '{message[2]}' with '{setup_results[2][0][0]}' as the Leader\n\n")

                serialized_result = pickle.dumps(setup_results)
                manager_socket.sendto(serialized_result, (peer_address, peer_port))

            elif command == "dht-complete":
                for peer in list_of_peers:
                    if peer["status"] == Leader and peer["name"] == message[1]:
                        print("dht-complete\n\n")
                big_prime = message[2]

            elif command == "query-dht":
                if not dht_setup_status:
                    print("A DHT has not been set up yet...")
                    print("FAILURE\n\n")
                else:
                    peer_to_query = message[1]
                    print(f"Peer '{peer_to_query}' is trying to query the DHT")
                    
                    if_peer_is_in = False
                    for peer in list_of_peers:
                        if peer["name"] == peer_to_query and (peer["status"] == Free):
                            if_peer_is_in = True

                    if not if_peer_is_in:
                        print("The Peer trying to query does not have a status of 'Free'")
                        print("FAILURE\n\n")
                    else:
                        query_peer = peers_in_dht[random.randint(0, len(peers_in_dht) - 1)]
                        print(f"Beginning the Hot Potato query from '{query_peer[0]}'\n\n")

                        msg_to_send = "query", "SUCCESS", query_peer, big_prime
                        manager_socket.sendto(pickle.dumps(msg_to_send), (peer_address, peer_port))

            elif command == "leave-dht":
                # message = ("leave-dht", "client_name_leaving", "new_leader_name")
                peer_to_leave = message[1]
                new_leader = message[2]
                print(f"Peer {peer_to_leave} is trying to leave the DHT")
                
                if_peer_is_in = False
                for peer in list_of_peers:
                    if peer["name"] == peer_to_leave and (peer["status"] == InDht or peer["status"] == Leader):
                        in_dht_entry_to_remove = (peer["name"], peer["ipv4_addr"], peer["p_port"])
                        if_peer_is_in = True

                if not dht_setup_status or not if_peer_is_in:
                    if not dht_setup_status:
                        print("The DHT is not setup")
                    else:
                        print(f"The Peer '{peer_to_leave}' is not in the DHT")
                    print("FAILURE\n\n")

                print(f"The peer '{peer_to_leave}' is in the DHT\nSending 'leave' command\n\n")

                leaving_peer = peer_to_leave
                leave_msg = "leave", leaving_peer, "SUCCESS"
                manager_socket.sendto(pickle.dumps(leave_msg), (peer_address, peer_port))

                # Remove the peer leaving from peers_in_dht
                peers_in_dht.remove(in_dht_entry_to_remove)

                # Change the leaving Peer's Status to Free and update the new leader status of the new leader and the old leader
                for peer in list_of_peers:
                    # print(f"Peer being checked: {peer}")
                    if peer["name"] == peer_to_leave:
                        # print("Peer leaving found. Status set to free\n\n")
                        peer["status"] = Free

                    if peer["status"] == Leader and peer["name"] != new_leader and peer["name"] != peer_to_leave:
                        # print("Updating the Status of the old leader to InDHT.\n\n")
                        peer["status"] = InDht
                    
                    if peer["name"] == new_leader:
                        # print("New Leader Found. Status set to Leader\n\n")
                        peer["status"] = Leader
                    

            elif command == "join-dht":
                if not dht_setup_status:
                    print("The DHT is not setup")
                    print("FAILURE")
                
                for peer in list_of_peers:
                    if peer["name"] == message[1] and peer["status"] != Free:
                        print(f"Peer {message[1]} is registered, but status is not 'Free'")
                        print("FAILURE")
                
                for peer in list_of_peers:
                    if peer["name"] == message[1]:
                        peer_to_join = (peer["name"], peer["ipv4_addr"], peer["p_port"])

                print(f"Peer '{message[1]}' status is 'Free'. Approved to join the DHT")
                print("SUCCESS\n\n")

                joining_peer = message[1]
                msg = "join", peer_to_join, peers_in_dht, "SUCCESS"
                manager_socket.sendto(pickle.dumps(msg), (peer_address, peer_port))

                # Append the newly joining peer to peers_in_dht
                peers_in_dht.append(peer_to_join)

                # Change the joining peer's status to InDHT
                for peers in list_of_peers:
                    # print(f"Peer being checked: {peers}")
                    if peers["name"] == peer_to_join[0]:
                        # print("Found the joining peer. Updating his status to InDHT\n\n")
                        peers["status"] = InDht

            elif command == "dht-rebuilt":
                print("dht-rebuilt'")
                print("SUCCESS\n\n")

            elif command == "deregister":
                peer_to_deregister = message[1]
        
                if_peer_is_in = False
                for peer in list_of_peers:
                    if peer["name"] == peer_to_deregister and (peer["status"] == InDht or peer["status"] == Leader):
                        if_peer_is_in = True
                
                if if_peer_is_in:
                    print(f"The Peer '{peer_to_deregister}' is in the DHT. Cannot deregister")
                    print("FAILURE\n\n")
                else:
                    for peer in list_of_peers:
                        if peer["name"] == peer_to_deregister:
                            rip_peer = {"name": peer["name"], "ipv4_addr": peer["ipv4_addr"], "m_port": peer["m_port"],
                                        "p_port": peer["p_port"],
                                        "status": peer["status"]}
                            list_of_peers.remove(rip_peer)
                            
                            print(f"Peer {peer_to_deregister} has deregistered")
                            print("SUCCESS\n\n")
                            
                            msg_to_send = "deregister", "SUCCESS"
                            manager_socket.sendto(pickle.dumps(msg_to_send), (peer_address, peer_port))

            elif command == "teardown-dht":
                leader_found = False
                for peer in list_of_peers:
                    if peer["name"] == message[1] and peer["status"] == Leader:
                        leader_found = True
                
                if not leader_found:
                    print(f"Peer '{message[1]}' initiating teardown-dht does not have status 'Leader'")
                    print("FAILURE\n\n")
                else:
                    print(f"Peer {message[1]} is initiating teardown-dht")
                    print("SUCCESS\n\n")

                    msg = "teardown-dht", "SUCCESS"
                    serialized_message = pickle.dumps(msg)
                    manager_socket.sendto(serialized_message, (peer_address, int(peer_port)))

                    # Change the joining peer's status to InDHT
                    for peers in list_of_peers:
                        peers["status"] = Free


            elif command == "teardown-complete":
                print("DHT torn down")
                print("SUCCESS\n\n")

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
        print("\n")
    elif message == "quit":
        print("Terminating Manager Explicitly\n")
