import socket

# Creates the Socket defining that we are using IPV4 and UDP Protocol
clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# binding the socket to the host IP and Port to listen and send on
clientSocket.bind(('192.168.0.116', 9090))
print("Binded socket")
messageToManager = ("Hello UDP Server... ").encode('utf-8')
clientSocket.sendto(messageToManager, ('10.0.2.15', 9999))
data, addr = clientSocket.recvfrom(4096)
print("Server says\n" + data.decode() + "\n")
print("Manager Response from:\n" +
      "IPV4 Address = " + addr[0] +
      "\nPort Number = " + str(addr[1]) + "\n\n")
clientSocket.close()
