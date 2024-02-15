import socket

import threading
import queue

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
    new_peer = {"name": peer_name, "ipv4_addr": ipv4_addr, "m_port": m_port, "p_port": p_port, "state": Free}

    for existing_peer in list_of_peers:
        if existing_peer["name"] == new_peer["name"] or existing_peer["m_port"] == new_peer["m_port"] \
                or existing_peer["p_port"] == new_peer["p_port"]:
            return "FAILURE"

    list_of_peers.append(new_peer)
    return "SUCCESS"


def set_up(peer_name, number_of_peers, year):  # setup-dht logic

    found = False
    for existing_peers in list_of_peers:  # looking for the peer that will become the leader
        found = peer_name in existing_peers["name"]
        if found:
            existing_peers["status"] = Leader  # updating his status
            break
    if not found or number_of_peers < 3 or peer_count < number_of_peers or dht_setup_status:  # if the parameters
        # violate the conditions fail
        return "FAILURE"

    list_of_sent_peers = []

    for free_peers in list_of_peers:  # choosing n-1 peers still confused how to do though randomly
        if free_peers["status"] == Free and len(list_of_sent_peers) < number_of_peers:  # choosing n-1 peers
            free_peers["status"] = InDht  # changing each peers status
            new_peer = {"name": free_peers["name"], "ipv4_addr": free_peers["ipv4_addr"],
                        "p_port": free_peers["p_port"]}  # creating the new dict with 3 relevant keys
            list_of_sent_peers.append(new_peer)  # appending it to a list
    return_tuple = tuple(list_of_sent_peers)  # converting the list to a tuple

    return "Success", return_tuple, year  # return the code and the tuple and the year


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

            # print(message)

            messageSplit = splitTheMessage(message)
            # print(messageSplit)
            command, parameters = messageSplit
            # print(command)

            parametersArray = parameters.split(" ")

            match command:
                case "register":
                    resultMessage = register(parametersArray[0], parametersArray[1], parametersArray[2],
                                             parametersArray[3])

                    manager_socket.sendto(resultMessage.encode(), (peer_address, peer_port))
                case _:
                    manager_socket.sendto("invalid command".encode(), (peer_address, peer_port))


t1 = threading.Thread(target=receive)
t2 = threading.Thread(target=broadcast)

t1.start()
t2.start()
