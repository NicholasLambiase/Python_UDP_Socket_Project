import socket

# NOTE ON IP ADDRESS SOCKET ASSIGNMENTS & SENDING MESSAGES
# When Communicating with machines on the same local network send messages to the PRIVATE IPV4 Address ie linux "hostname -I"
# When Communicating with machines NOT on the same local network send messages to the PUBLIC IPV4 Address ie go to whatismypublicIp.com

# Constants that will be defined in final implementation
HOST_IP = '127.0.0.1'         #These values will be redefined in final implementation ie Linux "hostname -I"
HOST_PORT = 12345             #Choose an arbitrary port #
ENCODING_TYPE = 'utf-8'       #encoding will always be utf-8

# Global Manager Variables
peerCount = 0
list_of_peers = []

managerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # Create the Socket passing its IPV4 Address and specifying UDP as the Protocol
managerSocket.bind((HOST_IP, HOST_PORT))  # binding the socket to the host IP and Port to listen and send on

print("Manager up and listening at:\n" +
      "IPV4 Address = " + HOST_IP +
      "\nPort Number = " + HOST_PORT + "\n\n")


# Manager functions
def register(peer_name, ipv4_addr, m_port, p_port):
    new_peer = {"name": peer_name, "ipv4_addr": ipv4_addr, "m_port": m_port, "p_port": p_port}

    for existing_peer in list_of_peers:
        if existing_peer["name"] == new_peer["name"] or existing_peer["m_port"] == new_peer["m_port"] \
                or existing_peer["p_port"] == new_peer["p_port"]:
            print("FAILURE")
            return

    list_of_peers.append(new_peer)
    print("SUCCESS")


while True:
    mssg, addr = managerSocket.recvfrom(4096)
    
	print("Client Message from:\n" +
      "IPV4 Address = " + addr[0] +
      "\nPort Number = " + str(addr[1]) + "\n\n")

    parsedMessage = mssg.split("_")

    
    
    response = ("This is the Response from the Manager Server").encode('utf-8')
    managerSocket.sendto(response, addr)

