import socket

# Creates the Socket defining that we are using IPV4 and UDP Protocol
managerSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# binding the socket to the host IP and Port to listen and send on
managerSocket.bind(('127.0.0.1',12345))

while True:
    mssg, addr = managerSocket.recvfrom(4096)
    print(mssg)
    response = ("Reached the Manager Server").encode('utf-8')
    managerSocket.sendto(response, addr)