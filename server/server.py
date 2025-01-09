import socket
import struct
import time
import threading

MAGIC_COOKIE = 0xabcddcba
MSG_TYPE_OFFER = 0x2
MSG_TYPE_REQUEST = 0x3
MSG_TYPE_PAYLOAD = 0x4

OFFER_FORMAT = '!IBHH'  # magic cookie, msg type, udp port, tcp port
REQUEST_FORMAT = '!IBQ'  # magic cookie, msg type, file size
PAYLOAD_FORMAT = '!IBQQ'  # magic cookie, msg type, total segments, current segment


def broadcast_offers(udp_port, tcp_port):
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    udp_socket.bind(('', 0))

    offer_message = struct.pack(OFFER_FORMAT, MAGIC_COOKIE, MSG_TYPE_OFFER, udp_port, tcp_port)
    
    while True:
        udp_socket.sendto(offer_message, ('<broadcast>', udp_port))
        print(f"Offer broadcast sent on UDP port {udp_port}")
        time.sleep(1)  # Sleep for 1 second to avoid busy-waiting
        
        
def handle_tcp_connection(client_socket, address):
    try:
        data = b""
        while not data.endswith(b"\n"):
            chunk = client_socket.recv(1024)
            if not chunk:
                break
            data += chunk
        requested_size = int(data.strip()) if data else 0
        payload = b"x" * requested_size
        client_socket.sendall(payload)
    finally:
        client_socket.close()


def handle_udp_requests(self, udp_socket):
    while True:
        data, client_addr = udp_socket.recvfrom(1024)
        magic_cookie, msg_type, requested_size = struct.unpack(self.REQUEST_FORMAT, data)
        
        if magic_cookie == self.MAGIC_COOKIE and msg_type == self.MSG_TYPE_REQUEST:
            chunk_size = 1024
            sequence_number = 0
            total_segments = (requested_size + chunk_size - 1) // chunk_size
            sent_bytes = 0
            
            while sent_bytes < requested_size:
                to_send = min(chunk_size, requested_size - sent_bytes)
                header = struct.pack(self.PAYLOAD_FORMAT, self.MAGIC_COOKIE, 
                                   self.MSG_TYPE_PAYLOAD, total_segments, sequence_number)
                packet = header + (b"x" * to_send)
                udp_socket.sendto(packet, client_addr)
                sequence_number += 1
                sent_bytes += to_send


def start_server(udp_port, tcp_port):
    # Start offer broadcasting in a thread
    threading.Thread(target=broadcast_offers, args=(udp_port, tcp_port), daemon=True).start()
    
    # TCP listener
    tcp_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_listener.bind(('', tcp_port))
    tcp_listener.listen()
    print(f"Server started, listening on IP address {tcp_listener.getsockname()[0]}")

    # UDP listener
    udp_listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_listener.bind(('', udp_port))
    udp_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    threading.Thread(target=handle_udp_requests, args=(udp_listener,), daemon=True).start()

    while True:
        client_socket, addr = tcp_listener.accept()
        threading.Thread(target=handle_tcp_connection, args=(client_socket, addr), daemon=True).start()