import socket

# Creates the Socket defining that we are using IPV4 and UDP Protocol
clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# binding the socket to the host IP and Port to listen and send on
clientSocket.bind(('127.0.100.2',12567))

messageToManager = ("Hello UDP Server... I am a peer that just contacted you!").encode('utf-8')
clientSocket.sendto(messageToManager, ('127.0.100.1', 12345))
data, addr = clientSocket.recvfrom(4096)
print("Server says\n" + data.decode() + "\n")
print("Manager Response from:\n" +
      "IPV4 Address = " + addr[0] +
      "\nPort Number = " + str(addr[1]) + "\n\n")
clientSocket.close()