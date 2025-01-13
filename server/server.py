import socket
import struct
import time
import threading
from colorama import init, Fore, Back, Style
import statistics
import datetime

# Initialize colorama
init()

MAGIC_COOKIE = 0xabcddcba
MSG_TYPE_OFFER = 0x2
MSG_TYPE_REQUEST = 0x3
MSG_TYPE_PAYLOAD = 0x4

OFFER_FORMAT = '!IBHH'  # magic cookie, msg type, udp port, tcp port
REQUEST_FORMAT = '!IBQ'  # magic cookie, msg type, file size
PAYLOAD_FORMAT = '!IBQQ'  # magic cookie, msg type, total segments, current segment


class ServerStatistics:
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.active_connections = 0
        self.total_connections = 0
        self.total_bytes_sent = 0
        self.transfer_speeds = []

    def add_speed_measurement(self, speed):
        self.transfer_speeds.append(speed)

    def print_summary(self):
        print(f"\n{Fore.CYAN}=== Server Statistics ==={Style.RESET_ALL}")
        uptime = datetime.datetime.now() - self.start_time
        print(f"{Fore.GREEN}Server Uptime: {uptime}{Style.RESET_ALL}")
        print(f"Active Connections: {self.active_connections}")
        print(f"Total Connections Handled: {self.total_connections}")
        print(f"Total Data Transferred: {self.total_bytes_sent / (1024 * 1024):.2f} MB")

        if self.transfer_speeds:
            print(f"\n{Fore.YELLOW}Transfer Speeds{Style.RESET_ALL}")
            print(f"Average: {statistics.mean(self.transfer_speeds):.2f} bits/sec")
            print(f"Max: {max(self.transfer_speeds):.2f} bits/sec")
            print(f"Min: {min(self.transfer_speeds):.2f} bits/sec")


# Global statistics object
server_stats = ServerStatistics()


def broadcast_offers(udp_port, tcp_port):
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    udp_socket.bind(('', 0))

    offer_message = struct.pack(OFFER_FORMAT, MAGIC_COOKIE, MSG_TYPE_OFFER, udp_port, tcp_port)

    while True:
        try:
            udp_socket.sendto(offer_message, ('<broadcast>', 13117))
            time.sleep(1)
        except Exception as e:
            print(f"{Fore.RED}Error broadcasting offer: {e}{Style.RESET_ALL}")


class UDPHandler:
    def __init__(self, udp_socket):
        self.udp_socket = udp_socket
        self.chunk_size = 1472

    def handle_requests(self):
        while True:
            try:
                data, client_addr = self.udp_socket.recvfrom(struct.calcsize(REQUEST_FORMAT))
                magic_cookie, msg_type, requested_size = struct.unpack(REQUEST_FORMAT, data)

                if magic_cookie == MAGIC_COOKIE and msg_type == MSG_TYPE_REQUEST:
                    print(f"{Fore.GREEN}Received UDP request from {client_addr[0]}:{client_addr[1]}{Style.RESET_ALL}")
                    threading.Thread(
                        target=self.handle_single_request,
                        args=(client_addr, requested_size),
                        daemon=True
                    ).start()

            except Exception as e:
                print(f"{Fore.RED}Error handling UDP request: {e}{Style.RESET_ALL}")
                continue

    def handle_single_request(self, client_addr, requested_size):
        server_stats.active_connections += 1
        server_stats.total_connections += 1
        start_time = time.time()

        try:
            chunk_size = self.chunk_size
            sequence_number = 0
            total_segments = (requested_size + chunk_size - 1) // chunk_size
            sent_bytes = 0
            data_chunk = b"x" * chunk_size

            print(f"{Fore.CYAN}Starting UDP transfer to {client_addr[0]}:{client_addr[1]}{Style.RESET_ALL}")

            while sent_bytes < requested_size:
                to_send = min(chunk_size, requested_size - sent_bytes)
                header = struct.pack(PAYLOAD_FORMAT, MAGIC_COOKIE,
                                     MSG_TYPE_PAYLOAD, total_segments, sequence_number)

                if to_send == chunk_size:
                    packet = header + data_chunk
                else:
                    packet = header + (b"x" * to_send)

                self.udp_socket.sendto(packet, client_addr)
                sequence_number += 1
                sent_bytes += to_send

                if sequence_number % 50 == 0:
                    time.sleep(0.001)

            duration = time.time() - start_time
            speed = (sent_bytes * 8) / duration if duration > 0 else 0
            server_stats.add_speed_measurement(speed)
            server_stats.total_bytes_sent += sent_bytes

            print(f"{Fore.GREEN}Completed UDP transfer to {client_addr[0]}:{client_addr[1]}{Style.RESET_ALL}")
            print(f"Sent {sent_bytes / 1024:.2f}KB in {duration:.2f} seconds ({speed / 1000000:.2f} Mbps)")

        except Exception as e:
            print(f"{Fore.RED}Error in UDP transfer to {client_addr[0]}:{client_addr[1]}: {e}{Style.RESET_ALL}")

        finally:
            server_stats.active_connections -= 1


def handle_tcp_connection(client_socket, address):
    server_stats.active_connections += 1
    server_stats.total_connections += 1
    start_time = time.time()

    try:
        print(f"{Fore.CYAN}New TCP connection from {address[0]}:{address[1]}{Style.RESET_ALL}")

        data = b""
        while not data.endswith(b"\n"):
            chunk = client_socket.recv(1024)
            if not chunk:
                break
            data += chunk

        requested_size = int(data.strip())
        chunk_size = 65536
        sent_bytes = 0
        data_chunk = b"x" * chunk_size

        client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        while sent_bytes < requested_size:
            to_send = min(chunk_size, requested_size - sent_bytes)
            if to_send == chunk_size:
                client_socket.sendall(data_chunk)
            else:
                client_socket.sendall(b"x" * to_send)
            sent_bytes += to_send

        duration = time.time() - start_time
        speed = (sent_bytes * 8) / duration if duration > 0 else 0
        server_stats.add_speed_measurement(speed)
        server_stats.total_bytes_sent += sent_bytes

        print(f"{Fore.GREEN}Completed TCP transfer to {address[0]}:{address[1]}{Style.RESET_ALL}")
        print(f"Sent {sent_bytes / 1024:.2f}KB in {duration:.2f} seconds ({speed / 1000000:.2f} Mbps)")

    except Exception as e:
        print(f"{Fore.RED}Error in TCP transfer to {address[0]}:{address[1]}: {e}{Style.RESET_ALL}")

    finally:
        client_socket.close()
        server_stats.active_connections -= 1

def get_free_port():
    """Get a random free port by letting the OS assign one"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port

def start_server(udp_port, tcp_port):
    try:
        # Try to bind TCP first
        tcp_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            tcp_listener.bind(('', tcp_port))
        except socket.error:
            print(f"{Fore.YELLOW}Could not bind to TCP port {tcp_port}, getting new port...{Style.RESET_ALL}")
            tcp_port = get_free_port()
            tcp_listener.bind(('', tcp_port))
        tcp_listener.listen()

        # Try to bind UDP
        udp_listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            udp_listener.bind(('', udp_port))
        except socket.error:
            print(f"{Fore.YELLOW}Could not bind to UDP port {udp_port}, getting new port...{Style.RESET_ALL}")
            udp_port = get_free_port()
            udp_listener.bind(('', udp_port))
        udp_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Get actual IP address
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        print(f"{Fore.GREEN}Server started, listening on IP address {ip_address}{Style.RESET_ALL}")
        print(f"TCP Port: {tcp_port}")
        print(f"UDP Port: {udp_port}")

        # Start UDP handler
        udp_handler = UDPHandler(udp_listener)
        udp_thread = threading.Thread(target=udp_handler.handle_requests, daemon=True)
        udp_thread.start()

        # Start broadcast thread
        broadcast_thread = threading.Thread(target=broadcast_offers, args=(udp_port, tcp_port), daemon=True)
        broadcast_thread.start()

        # Print statistics periodically
        def print_stats():
            while True:
                time.sleep(60)  # Print stats every minute
                server_stats.print_summary()

        stats_thread = threading.Thread(target=print_stats, daemon=True)
        stats_thread.start()

        # Handle TCP connections in main thread
        while True:
            client_socket, addr = tcp_listener.accept()
            threading.Thread(target=handle_tcp_connection, args=(client_socket, addr), daemon=True).start()

    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Server shutting down...{Style.RESET_ALL}")
        server_stats.print_summary()
    except Exception as e:
        print(f"{Fore.RED}Fatal error: {e}{Style.RESET_ALL}")
        raise