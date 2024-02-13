import socket
import struct

# Constants that will be defined in final implementation
HOST_IP = '127.0.0.1'   #These values will be redefined in final implementation
HOST_PORT = 12345
ENCODING_TYPE = 'utf-8'

# Define Structs that will be used to store registered peers





# Create the Socket passing its IPV4 Address and specifying UDP as the Protocol
managerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# binding the socket to the host IP and Port to listen and send on
managerSocket.bind(('127.0.0.1',12345))

print("Manager up and listening at:\n" +
      "IPV4 Address = " + HOST_IP +
      "\nPort Number = " + HOST_PORT + "\n\n")

