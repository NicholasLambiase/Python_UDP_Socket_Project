import socket

# Creates the Socket defining that we are using IPV4 and UDP Protocol
managerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# binding the socket to the host IP and Port to listen and send on
managerSocket.bind(('127.0.0.1', 9999))

print("Manager up and listening")

while True:
    mssg, addr = managerSocket.recvfrom(4096)
    print(mssg.decode() + "\n")
    print("Client Message from:\n" +
      "IPV4 Address = " + addr[0] +
      "\nPort Number = " + str(addr[1]) + "\n\n")
    
    response = ("This is the Response from the Manager Server").encode('utf-8')
    managerSocket.sendto(response, addr)