import socket

import threading
import queue
import random

# Port range 18,000 to 18,499

# Defining Constants;
Free = "free"
Leader = "leader"
InDht = "indht"
HOST_IP = "localhost"
HOST_PORT = 9996
ENCODER = "utf-8"
PEER_NAME_SIZE = 15

messages = queue.Queue()
users = []
list_of_peers = []
dht_setup_status = False

peer_count = 0

manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

manager_socket.bind((HOST_IP, HOST_PORT))


#
# print("server is up...\n")
# print("\t\t\t===> COM Channel <===\n")
# print("Type 'quit' to exit")
def splitTheMessage(fullMessage):
    command = fullMessage.split(" ", 1)
    return command


def register(peer_name, ipv4_addr, m_port, p_port):
    new_peer = {"name": peer_name, "ipv4_addr": ipv4_addr, "m_port": m_port, "p_port": p_port, "status": Free}

    for existing_peer in list_of_peers:
        if existing_peer["name"] == new_peer["name"] or existing_peer["m_port"] == new_peer["m_port"] \
                or existing_peer["p_port"] == new_peer["p_port"]:
            return "FAILURE"

    list_of_peers.append(new_peer)
    global peer_count
    peer_count += 1
    return "SUCCESS"


def set_up(peer_name, number_of_peers, year):  # setup-dht logic
    list_of_sent_peers = []
    found = False
    for existing_peers in list_of_peers:  # looking for the peer that will become the leader
        found = peer_name in existing_peers["name"]
        if found:
            existing_peers["status"] = Leader  # updating his status
            new_leader = {"name": existing_peers["name"], "ipv4_addr": existing_peers["ipv4_addr"],
                          "p_port": existing_peers["p_port"]}  # setting up the leader info
            list_of_sent_peers.append(new_leader)  # adding the leader info
            break

    if not found or int(number_of_peers) < 3 or peer_count < int(
            number_of_peers) or dht_setup_status:  # if the parameters
        # violate the conditions fail
        return "FAILURE"

    while len(list_of_sent_peers) < int(number_of_peers):

        possible_peer = list_of_peers[random.randint(0, peer_count)]

        if possible_peer["status"] == Free:
            possible_peer["status"] = InDht  # changing each peers status
            new_peer = {"name": possible_peer["name"], "ipv4_addr": possible_peer["ipv4_addr"],
                        "p_port": possible_peer["p_port"]}  # creating the new dict with 3 relevant keys
            list_of_sent_peers.append(new_peer)  # appending it to a list

    return_tuple = tuple(list_of_sent_peers)  # converting the list to a tuple

    return "SUCCESS", return_tuple, year  # return the code and the tuple and the year


# register nick 127.0.0.1 21 91
# register sam 127.0.0.2 22 92
# register oprah 127.0.0.3 23 93
# register phill 127.0.0.4 24 94
# register liam 127.0.0.5 25 95
# register chris 127.0.0.6 26 96
# register arin 127.0.0.7 27 97
# register danny 127.0.0.8 28 98

# set_up("nick", "3", "1955")


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

            message, addr = messages.get()

            peer_address, peer_port = addr

            message = message.decode().strip()

            messageSplit = splitTheMessage(message)

            command, parameters = messageSplit

            parametersArray = parameters.split(" ")

            match command:
                case "register":
                    resultMessage = register(parametersArray[0], parametersArray[1], parametersArray[2],
                                             parametersArray[3])

                    manager_socket.sendto(resultMessage.encode(), (peer_address, peer_port))
                case "setup-dht":
                    resultMessage = set_up(parametersArray[0], parametersArray[1], parametersArray[2])
                    #print(resultMessage)
                    manager_socket.sendto(resultMessage.encode(), (peer_address, peer_port))
                case _:
                    manager_socket.sendto("invalid command".encode(), (peer_address, peer_port))


t1 = threading.Thread(target=receive)
t2 = threading.Thread(target=broadcast)

t1.start()
t2.start()
