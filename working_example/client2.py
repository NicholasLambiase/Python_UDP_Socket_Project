import socket

# Creates the Socket defining that we are using IPV4 and UDP Protocol
clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# binding the socket to the host IP and Port to listen and send on
clientSocket.bind(('127.0.0.2', 9090))

messageToManager = ("Hello UDP Server... ").encode('utf-8')
clientSocket.sendto(messageToManager, ('127.0.0.1', 9999))
data, addr = clientSocket.recvfrom(4096)
print("Server says\n" + data.decode() + "\n")
print("Manager Response from:\n" +
      "IPV4 Address = " + addr[0] +
      "\nPort Number = " + str(addr[1]) + "\n\n")
clientSocket.close()