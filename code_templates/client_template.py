import socket

# Constants that will be defined in final implementation
HOST_IP = '127.0.0.2'       #These values will be redefined in final implementation ie Linux "hostname -I"
M_PORT = 12346              #Choose an arbitrary port #
P_PORT = 13457
ENCODING_TYPE = 'utf-8'     #encoding will always be utf-8

MANAGER_IP_ADDRESS = '127.0.0.1'
MANAGER_PORT = 12345

p_to_p_Socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # Creates the Socket defining that we are using IPV4 and UDP Protocol
p_to_p_Socket.bind((HOST_IP, P_PORT))   # binding the socket to the host IP and Port to listen and send on

p_to_m_Socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # Creates the Socket defining that we are using IPV4 and UDP Protocol
p_to_m_Socket.bind((HOST_IP, M_PORT))   # binding the socket to the host IP and Port to listen and send on

while True:
    peerCommand = input()
    match peerCommand:
        case "register":
            registerMessage = peerCommand + "_" + M_PORT + "_" + P_PORT
            p_to_m_Socket.sendto(registerMessage.encode(ENCODING_TYPE), (MANAGER_IP_ADDRESS, MANAGER_PORT))
        case "done":
            p_to_p_Socket.close()
            p_to_m_Socket.close()
            break
        case _:
            print("Enter a Vaild Input")

print("Sockets Closed...\nScript Finished")