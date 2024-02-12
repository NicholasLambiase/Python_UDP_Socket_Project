# this will be the manager code
# you are a nerd times 2
from dataclasses import dataclass

# Defining Constants;
Free = "free"
Leader = "leader"
InDht = "indht"

PEER_NAME_SIZE = 15


@dataclass
class Peer:
    Peer_Name: str
    Ipv4_addr: str
    m_port: int
    p_port: int


# playing around with a struct
peer = Peer("Nick", "192.168.1.0", 20, 9000)

list_of_peers = [{"name": "nick", "ipv4_addr": "192.168.1.0", "m_port": 20, "p_port": 9000}]


def register(peer_name, ipv4_addr, m_port, p_port):

    new_peer = {"name": peer_name, "ipv4_addr": ipv4_addr, "m_port": m_port, "p_port": p_port}

    for existing_peer in list_of_peers:
        if existing_peer["name"] == new_peer["name"] or existing_peer["m_port"] == new_peer["m_port"]\
                or existing_peer["p_port"] == new_peer["p_port"]:
            print("FAILURE")
            return

    list_of_peers.append(new_peer)
    print("SUCCESS")


# Example usage:
register("john", "192.168.1.1", 30, 9500)
register("paul", "192.168.1.0", 30, 9999)
