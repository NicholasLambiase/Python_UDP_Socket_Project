import socket
from dataclasses import dataclass
import threading
import queue

# Port range 18,000 to 18,499

# Defining Constants;
Free = "free"
Leader = "leader"
InDht = "indht"
HOST_IP = "localhost"
HOST_PORT = 9998
ENCODER = "utf-8"
PEER_NAME_SIZE = 15

messages = queue.Queue()
users = []

manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

manager_socket.bind((HOST_IP, HOST_PORT))
#
# print("server is up...\n")
# print("\t\t\t===> COM Channel <===\n")
# print("Type 'quit' to exit")


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
            message, addr = messages.get()
            print(message.decode())
            parsed_message = message.decode().split(" ")

            match parsed_message[0]:




t1 = threading.Thread(target=receive)
t2 = threading.Thread(target=broadcast)

t1.start()
t2.start()


# @dataclass
# class Peer:
#     Peer_Name: str
#     Ipv4_addr: str
#     m_port: int
#     p_port: int
#
#
# # playing around with a struct
# peer = Peer("Nick", "192.168.1.0", 20, 9000)

list_of_peers = [{"name": "nick", "ipv4_addr": "192.168.1.0", "m_port": 20, "p_port": 9000}]


def register(peer_name, ipv4_addr, m_port, p_port):
    new_peer = {"name": peer_name, "ipv4_addr": ipv4_addr, "m_port": m_port, "p_port": p_port}

    for existing_peer in list_of_peers:
        if existing_peer["name"] == new_peer["name"] or existing_peer["m_port"] == new_peer["m_port"] \
                or existing_peer["p_port"] == new_peer["p_port"]:
            print("FAILURE")
            return

    list_of_peers.append(new_peer)
    print("SUCCESS")
