import random
import socket
import sys
import time
from server.server import start_server
from client.client import SpeedTestClient

def get_free_port():
    """Get a random free port by letting the OS assign one"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port

def main():
    if len(sys.argv) != 2:
        print("Usage: python main.py [server|client]")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "server":
        # Get available ports from OS
        tcp_port = get_free_port()
        udp_port = get_free_port()

        print(f"Starting server with TCP port: {tcp_port}, UDP port: {udp_port}")
        try:
            start_server(udp_port, tcp_port)
        except Exception as e:
            print(f"Error starting server: {e}")
            sys.exit(1)

    elif mode == "client":
        client = SpeedTestClient()
        client.run()

    else:
        print("Invalid mode. Use 'server' or 'client'")
        sys.exit(1)

if __name__ == "__main__":
    main()