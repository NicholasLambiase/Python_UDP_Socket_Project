import socket
import threading
import queue
import random
import pickle

# Port range 18,000 to 18,499

# Defining Constants;
Free = "free"
Leader = "leader"
InDht = "indht"
HOST_IP = "localhost"
HOST_PORT = 9995
ENCODER = "utf-8"
PEER_NAME_SIZE = 15

# Global
messages = queue.Queue()
users = []
list_of_peers = []
dht_setup_status = False
peer_count = 0

# Setting Up the Socket
manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
manager_socket.bind((HOST_IP, HOST_PORT))


# Splits the message rec
def split_the_message(full_message):
    split_message = full_message.split(" ", 1)
    return split_message


def send_message_to_client(peer_address, peer_port, obj_to_send):
    serialized_obj = pickle.dumps(obj_to_send)


def register(peer_name, ipv4_addr, m_port, p_port):
    new_peer = {"name": peer_name, "ipv4_addr": ipv4_addr, "m_port": m_port, "p_port": p_port, "status": Free}

    for existing_peer in list_of_peers:
        if existing_peer["name"] == new_peer["name"] or existing_peer["p_port"] == new_peer["p_port"]: # or
            # existing_peer["m_port"] == new_peer["m_port"]

            return "FAILURE"

    list_of_peers.append(new_peer)
    global peer_count
    peer_count += 1
    return "SUCCESS"


def set_up(peer_name, number_of_peers, year):  # setup-dht logic
    list_of_sent_peers = []

    # looking for the peer that will become the leader
    found = False
    if int(number_of_peers) < 3 or peer_count < int(
            number_of_peers) or dht_setup_status:  # if the parameters
        # violate the conditions fail
        return "FAILURE"

    for existing_peers in list_of_peers:  # looking for the peer that will become the leader  
        found = peer_name in existing_peers["name"]
        if found:
            existing_peers["status"] = Leader  # updating his status
            new_leader = (existing_peers["name"], existing_peers["ipv4_addr"], existing_peers["p_port"])  # setting
            # up the leader info
            list_of_sent_peers.append(new_leader)  # adding the leader info
            break

    if not found:
        return "FAILURE"

    while len(list_of_sent_peers) < int(number_of_peers):

        possible_peer = list_of_peers[random.randint(0, peer_count - 1)]

        if possible_peer["status"] == Free:
            possible_peer["status"] = InDht  # changing each peers status
            new_peer = (possible_peer["name"], possible_peer["ipv4_addr"], possible_peer["p_port"])  # creating the
            # new tuple with 3 values
            list_of_sent_peers.append(new_peer)  # appending it to a list

    return "SUCCESS", list_of_sent_peers, year  # return the code and the tuple and the year


# register("nick", "localhost", "9995", str(random.randint(18000, 18499)))
# register("sam", "localhost", "9995", str(random.randint(18000, 18499)))
# register("oprah", "localhost", "9995", str(random.randint(18000, 18499)))
# register("phill", "localhost", "9995", str(random.randint(18000, 18499)))
# register("liam", "localhost", "9995", str(random.randint(18000, 18499)))
# register("chris", "localhost", "9995", str(random.randint(18000, 18499)))
# register("arin", "localhost", "9995", str(random.randint(18000, 18499)))
# register("danny", "localhost", "9995", str(random.randint(18000, 18499)))
#
# message = set_up("nick", "3", "1955")
#
# print(message)


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
            command, parameters = split_the_message(message)

            parameters_array = parameters.split(" ")

            match command:
                case "register":

                    result_message = register(parameters_array[0], parameters_array[1], parameters_array[2],
                                              parameters_array[3])

                    # Debugging Message
                    print(result_message)

                    pickled_message = pickle.dumps(result_message)
                    manager_socket.sendto(pickled_message, (peer_address, peer_port))
                case "setup-dht":
                    setup_results = set_up(parameters_array[0], parameters_array[1], parameters_array[2])
                    serialized_result = pickle.dumps(setup_results)

                    # Debugging Message
                    print(setup_results)

                    manager_socket.sendto(serialized_result, (peer_address, peer_port))

                case _:
                    manager_socket.sendto("invalid command".encode(), (peer_address, peer_port))


t1 = threading.Thread(target=receive)
t2 = threading.Thread(target=broadcast)

t1.start()
t2.start()
