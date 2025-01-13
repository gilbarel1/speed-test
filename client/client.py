import socket
import struct
import time
import threading
from colorama import init, Fore, Back, Style
from tqdm import tqdm
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


class ClientState:
    STARTUP = 1
    LOOKING_FOR_SERVER = 2
    SPEED_TEST = 3


class Statistics:
    def __init__(self):
        self.transfer_speeds = []
        self.packet_loss_rates = []
        self.latencies = []
        self.start_time = None
        self.connection_attempts = 0
        self.successful_connections = 0
        self.total_bytes_received = 0

    def add_speed_measurement(self, speed):
        self.transfer_speeds.append(speed)

    def add_packet_loss(self, loss_rate):
        self.packet_loss_rates.append(loss_rate)

    def add_latency(self, latency):
        self.latencies.append(latency)


    def print_summary(self):
        print("\n" + "=" * 50)
        print(f"{Fore.CYAN}=== Test Summary ==={Style.RESET_ALL}")
        print("=" * 50 + "\n")

        if self.transfer_speeds:
            print(f"{Fore.GREEN}Transfer Speeds{Style.RESET_ALL}")
            print(f"  Average: {statistics.mean(self.transfer_speeds)/1000000:8.2f} Mbps")
            print(f"  Min:     {min(self.transfer_speeds)/1000000:8.2f} Mbps")
            print(f"  Max:     {max(self.transfer_speeds)/1000000:8.2f} Mbps")
            print()

        if self.packet_loss_rates:
            print(f"{Fore.YELLOW}Packet Loss{Style.RESET_ALL}")
            print(f"  Average: {statistics.mean(self.packet_loss_rates):8.2f}%")
            print()

        if self.latencies:
            print(f"\n{Fore.MAGENTA}Latency{Style.RESET_ALL}")
            print(f"  Average: {statistics.mean(self.latencies):.2f} ms")
            print(f"  Jitter: {statistics.stdev(self.latencies):.2f} ms")

        print(f"{Fore.BLUE}Connection Statistics{Style.RESET_ALL}")
        success_rate = (self.successful_connections / self.connection_attempts * 100) if self.connection_attempts > 0 else 0
        print(f"  Success Rate:       {success_rate:8.1f}%")
        print(f"  Total Data:         {self.total_bytes_received / (1024*1024):8.2f} MB")
        print("\n" + "="*50)


class SpeedTestClient:
    def __init__(self):
        self.state = ClientState.STARTUP
        self.file_size = 0
        self.tcp_connections = 0
        self.udp_connections = 0
        self.server_ip = None
        self.udp_port = None
        self.tcp_port = None
        self.stats = Statistics()
        self.print_lock = threading.Lock()

    def update_progress(self, pbar, bytes_received, speed=None):
        pbar.update(bytes_received)
        if speed:
            pbar.set_postfix_str(f"{speed / 1000000:.2f} Mbps", refresh=True)

    def safe_print(self, message):
        with self.print_lock:
            print(message)

    def get_user_parameters(self):
        """Get file size and connection counts from user with validation"""
        print(f"{Fore.CYAN}Enter test parameters:{Style.RESET_ALL}")
        while True:
            try:
                file_size = input(f"{Fore.GREEN}File size (bytes):{Style.RESET_ALL} ")
                self.file_size = int(file_size)
                if self.file_size <= 0:
                    raise ValueError("File size must be positive")

                tcp_conn = input(f"{Fore.GREEN}TCP connections:{Style.RESET_ALL} ")
                self.tcp_connections = int(tcp_conn)
                if self.tcp_connections < 0:
                    raise ValueError("TCP connections cannot be negative")

                udp_conn = input(f"{Fore.GREEN}UDP connections:{Style.RESET_ALL} ")
                self.udp_connections = int(udp_conn)
                if self.udp_connections < 0:
                    raise ValueError("UDP connections cannot be negative")

                if self.tcp_connections + self.udp_connections == 0:
                    raise ValueError("At least one connection type must be specified")

                break
            except ValueError as e:
                print(f"{Fore.RED}Invalid input: {str(e)}{Style.RESET_ALL}")

        self.state = ClientState.LOOKING_FOR_SERVER

    def listen_for_offers(self):
        print(f"{Fore.YELLOW}Client started, Listening for server offers...{Style.RESET_ALL}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, 'SO_REUSEPORT'):  # Not available on all platforms
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(('', 13117))

        while self.state == ClientState.LOOKING_FOR_SERVER:
            try:
                data, addr = sock.recvfrom(struct.calcsize(OFFER_FORMAT))
                magic_cookie, msg_type, udp_port, tcp_port = struct.unpack(OFFER_FORMAT, data)

                if magic_cookie == MAGIC_COOKIE and msg_type == MSG_TYPE_OFFER:
                    self.server_ip = addr[0]
                    self.udp_port = udp_port
                    self.tcp_port = tcp_port
                    print(f"Received offer from {self.server_ip}")
                    self.state = ClientState.SPEED_TEST
                    return True
            except Exception as e:
                print(f"Error receiving offer: {e}")
        return False

    def start_speed_test(self):
        """Launch parallel TCP and UDP transfers"""
        print(f"{Fore.CYAN}Starting speed test with server {self.server_ip}{Style.RESET_ALL}")
        threads = []

        # Start TCP transfers
        for i in range(self.tcp_connections):
            t = threading.Thread(target=self.handle_tcp_transfer, args=(i + 1,))
            threads.append(t)
            t.start()

        # Start UDP transfers
        for i in range(self.udp_connections):
            t = threading.Thread(target=self.handle_udp_transfer, args=(i + 1,))
            threads.append(t)
            t.start()

        # Wait for completion
        for t in threads:
            t.join()

        print(f"{Fore.GREEN}All transfers complete, listening to offer requests{Style.RESET_ALL}")
        self.state = ClientState.LOOKING_FOR_SERVER

    def handle_tcp_transfer(self, transfer_num):
        try:
            self.stats.connection_attempts += 1
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(5)

            print(f"{Fore.YELLOW}TCP #{transfer_num}: Connecting to {self.server_ip}:{self.tcp_port}{Style.RESET_ALL}")
            sock.connect((self.server_ip, self.tcp_port))
            print(f"{Fore.CYAN}TCP #{transfer_num}: Connected successfully{Style.RESET_ALL}")

            # Send request
            request = str(self.file_size).encode() + b"\n"
            sock.send(request)

            start_time = time.time()
            received = 0
            chunk_size = 65536

            # Create progress bar
            pbar = tqdm(
                total=self.file_size,
                unit='B',
                unit_scale=True,
                desc=f"TCP #{transfer_num}",
                bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
                position=transfer_num - 1,
                leave=False  # Don't leave the bar after completion
            )

            while received < self.file_size:
                data = sock.recv(chunk_size)
                if not data:
                    break
                received += len(data)
                elapsed = time.time() - start_time
                speed = (received * 8) / elapsed if elapsed > 0 else 0
                self.update_progress(pbar, len(data), speed)

            pbar.close()
            duration = time.time() - start_time
            speed = (received * 8) / duration if duration > 0 else 0
            speed_mbps = speed / 1_000_000

            if received > 0:
                self.stats.add_speed_measurement(speed)
                self.stats.successful_connections += 1
                self.stats.total_bytes_received += received

                print(f"{Fore.GREEN}TCP #{transfer_num} transfer finished{Style.RESET_ALL}")
                print(f"  Total Time: {duration:.2f} seconds")
                print(f"  Total Speed: {speed_mbps:.2f} Mbps")
                print(
                    f"{Fore.GREEN}TCP transfer #{transfer_num} finished, total time: {duration:.2f} seconds, total speed: {speed:.2f} bits/second{Style.RESET_ALL}")

        except Exception as e:
            print(f"{Fore.RED}Error in TCP transfer #{transfer_num}: {str(e)}{Style.RESET_ALL}")

        finally:
            try:
                sock.close()
            except:
                pass

    def handle_udp_transfer(self, transfer_num):
        try:
            self.stats.connection_attempts += 1
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(1.0)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16777216)

            print(
                f"{Fore.YELLOW}UDP #{transfer_num}: Starting transfer with {self.server_ip}:{self.udp_port}{Style.RESET_ALL}")

            request = struct.pack(REQUEST_FORMAT, MAGIC_COOKIE, MSG_TYPE_REQUEST, self.file_size)
            sock.sendto(request, (self.server_ip, self.udp_port))

            start_time = time.time()
            received_packets = {}
            expected_segments = None
            total_received = 0
            last_receive_time = time.time()
            chunk_size = 1472

            # Create progress bar for UDP
            pbar = tqdm(
                total=self.file_size,
                unit='B',
                unit_scale=True,
                desc=f"UDP #{transfer_num}",
                bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
                position=self.tcp_connections + transfer_num - 1,  # Position after TCP bars
                leave=False
            )

            while True:
                try:
                    data = sock.recv(chunk_size + struct.calcsize(PAYLOAD_FORMAT))
                    current_time = time.time()
                    last_receive_time = current_time

                    header = data[:struct.calcsize(PAYLOAD_FORMAT)]
                    magic_cookie, msg_type, total_segs, curr_seg = struct.unpack(PAYLOAD_FORMAT, header)

                    if magic_cookie == MAGIC_COOKIE and msg_type == MSG_TYPE_PAYLOAD:
                        if expected_segments is None:
                            expected_segments = total_segs
                        payload = data[struct.calcsize(PAYLOAD_FORMAT):]
                        received_packets[curr_seg] = payload
                        total_received += len(payload)
                        elapsed = time.time() - start_time
                        speed = (total_received * 8) / elapsed if elapsed > 0 else 0
                        self.update_progress(pbar, len(payload), speed)

                        if total_received >= self.file_size:
                            break

                except socket.timeout:
                    if time.time() - last_receive_time >= 1.0:
                        break
                    continue

            pbar.close()
            duration = time.time() - start_time
            success_rate = (len(received_packets) / expected_segments * 100) if expected_segments else 0
            speed = (total_received * 8) / duration if duration > 0 else 0

            self.stats.add_speed_measurement(speed)
            self.stats.add_packet_loss(100 - success_rate)
            self.stats.successful_connections += 1
            self.stats.total_bytes_received += total_received

            print(f"{Fore.GREEN}UDP #{transfer_num} transfer finished{Style.RESET_ALL}")
            print(f"  Total Time: {duration:.2f} seconds")
            print(f"  Total Speed: {speed / 1000000:.2f} Mbps")
            print(f"  percentage of packets received successfully:  {success_rate:.1f}%")

        except Exception as e:
            print(f"{Fore.RED}Error in UDP transfer #{transfer_num}: {str(e)}{Style.RESET_ALL}")

        finally:
            sock.close()

    def run(self):
        """Main client loop with improved error handling and statistics"""
        self.stats.start_time = datetime.datetime.now()
        try:
            while True:
                if self.state == ClientState.STARTUP:
                    self.get_user_parameters()

                elif self.state == ClientState.LOOKING_FOR_SERVER:
                    if self.listen_for_offers():
                        self.start_speed_test()
                        self.stats.print_summary()
                        self.state = ClientState.LOOKING_FOR_SERVER  # Reset state to continue listening
                        print(f"\n{Fore.YELLOW}Client started, Listening for server offers...{Style.RESET_ALL}")

                elif self.state == ClientState.SPEED_TEST:
                    print(f"{Fore.GREEN}Test complete{Style.RESET_ALL}")
                    self.state = ClientState.LOOKING_FOR_SERVER

        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}Test interrupted by user{Style.RESET_ALL}")
            self.stats.print_summary()
        except Exception as e:
            print(f"{Fore.RED}Error: {str(e)}{Style.RESET_ALL}")