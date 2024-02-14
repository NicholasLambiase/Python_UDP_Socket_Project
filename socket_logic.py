import socket
from dataclasses import dataclass
import threading
import queue

# Global Variables
list_of_peers = []

# Peer States
Free = "free"
Leader = "leader"
InDht = "indht"

# Queue That will hold all incoming messages from peers
messages = queue.Queue()

# Manager Network Constants
HOST_IP = "10.0.2.15"
HOST_PORT = 9999
ENCODER = "utf-8"

# Creating and Binding a UDP Socket to our Private IPv4 Address
manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
manager_socket.bind((HOST_IP, HOST_PORT))

def splitTheMessage(fullMessage):
    command = fullMessage.split(" ", 1)
    return command

def register(peer_name, ipv4_addr, m_port, p_port):
    new_peer = {"name": peer_name, "ipv4_addr": ipv4_addr, "m_port": m_port, "p_port": p_port}

    for existing_peer in list_of_peers:
        if existing_peer["name"] == new_peer["name"] or existing_peer["m_port"] == new_peer["m_port"] \
                or existing_peer["p_port"] == new_peer["p_port"]:
            return "FAILURE"

    list_of_peers.append(new_peer)
    return "SUCCESS"

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
            

            messageSplit = splitTheMessage(message)
            command, parameters = messageSplit

            parametersArray = parameters.split(" ")

            match command:
                case "register":
                    resultMessage = register(parametersArray[0], parametersArray[1], parametersArray[2], parametersArray[3])
                    manager_socket.sendto(resultMessage, (peer_address, peer_port))
                case _:
                    manager_socket.sendto("", (peer_address, peer_port))
                    
            


t1 = threading.Thread(target=receive)
t2 = threading.Thread(target=broadcast)

t1.start()
t2.start()