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

class ClientState:
    STARTUP = 1
    LOOKING_FOR_SERVER = 2
    SPEED_TEST = 3
    

class SpeedTestClient:
    def __init__(self):
        self.state = ClientState.STARTUP
        self.file_size = 0
        self.tcp_connections = 0
        self.udp_connections = 0
        self.server_ip = None
        self.udp_port = None
        self.tcp_port = None
        
    def get_user_parameters(self):
        """Get file size and connection counts from user"""
        print("Enter test parameters:")
        self.file_size = int(input("File size (bytes): "))
        self.tcp_connections = int(input("TCP connections: "))
        self.udp_connections = int(input("UDP connections: "))
        self.state = ClientState.LOOKING_FOR_SERVER
        
    def listen_for_offers(self):
        """Listen for server offers"""
        print("Client started, listening for offer requests...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
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
        threads = []

        # Start TCP transfers
        for i in range(self.tcp_connections):
            t = threading.Thread(target=self.handle_tcp_transfer, args=(i+1,))
            threads.append(t)
            t.start()

        # Start UDP transfers    
        for i in range(self.udp_connections):
            t = threading.Thread(target=self.handle_udp_transfer, args=(i+1,))
            threads.append(t)
            t.start()

        # Wait for completion
        for t in threads:
            t.join()
            
            
    def handle_tcp_transfer(self, transfer_num):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.server_ip, self.tcp_port))
            
            # Send request
            request = f"{self.file_size}\n".encode()
            sock.send(request)
            
            start_time = time.time()
            received = 0
            
            while received < self.file_size:
                data = sock.recv(1024 + struct.calcsize(PAYLOAD_FORMAT))
                if not data:
                    break
                # Unpack header
                header = data[:struct.calcsize(PAYLOAD_FORMAT)]
                magic_cookie, msg_type, total_segs, curr_seg = struct.unpack(PAYLOAD_FORMAT, header)
                
                if magic_cookie == MAGIC_COOKIE and msg_type == MSG_TYPE_PAYLOAD:  
                    payload = data[struct.calcsize(PAYLOAD_FORMAT):]
                    received += len(payload)
                else:
                    print("Invalid packet received")
                    break
                
            duration = time.time() - start_time
            speed = (received * 8) / duration
            print(f"TCP transfer #{transfer_num} finished, total time: {duration:.2f} seconds, total speed: {speed:.1f} bits/second")
        
        finally:
            sock.close()
            
            
    def handle_udp_transfer(self, transfer_num):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(1.0)
            
            # Send request packet
            request = struct.pack(REQUEST_FORMAT, MAGIC_COOKIE, MSG_TYPE_REQUEST, self.file_size)
            sock.sendto(request, (self.server_ip, self.udp_port))
            
            start_time = time.time()
            received_packets = set()
            total_packets = 0
            
            while True:
                try:
                    data = sock.recv(1024 + struct.calcsize(PAYLOAD_FORMAT))
                    header = data[:struct.calcsize(PAYLOAD_FORMAT)]
                    magic_cookie, msg_type, total_segs, curr_seg = struct.unpack(PAYLOAD_FORMAT, header)
                    
                    if magic_cookie == MAGIC_COOKIE and msg_type == MSG_TYPE_PAYLOAD:
                        received_packets.add(curr_seg)
                        total_packets += 1
                    else:
                        print("Invalid packet received")
                        break         
                except socket.timeout:
                    break
                    
            duration = time.time() - start_time
            success_rate = (len(received_packets) / total_packets * 100) if total_packets > 0 else 0
            speed = (len(received_packets) * 1024 * 8) / duration
            
            print(f"UDP transfer #{transfer_num} finished, total time: {duration:.2f} seconds, "
                f"total speed: {speed:.1f} bits/second, percentage of packets received successfully: {success_rate:.1f}%")
              
        finally:
            sock.close()

    def run(self):
        """Main client loop"""
        while True:
            if self.state == ClientState.STARTUP:
                self.get_user_parameters()
                
            elif self.state == ClientState.LOOKING_FOR_SERVER:
                if self.listen_for_offers():
                    self.start_speed_test()
                    
            elif self.state == ClientState.SPEED_TEST:
                print("All transfers complete, listening to offer requests")
                self.state = ClientState.LOOKING_FOR_SERVER