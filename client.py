"""
client.py - Terminal-based Peer-to-Peer Chat Client.

Connects to a signaling server solely for peer discovery and network-endpoint exchange.
All chat communication is strictly direct peer-to-peer over dedicated TCP sockets.
"""

import os
import sys
import time
import socket
import logging
import threading
from typing import Dict, Any, Optional

from config import AppConfig, get_lan_ip, validate_ip, validate_port
from protocol import (
    send_msg,
    recv_msg,
    ACTION_REGISTER,
    ACTION_REGISTER_ACK,
    ACTION_REGISTER_ERROR,
    ACTION_LIST_USERS,
    ACTION_USER_LIST,
    ACTION_CONNECT_REQ,
    ACTION_PEER_INFO,
    ACTION_CONNECT_ERROR,
    ACTION_USER_DISCONNECTED,
    ACTION_PEER_HANDSHAKE,
    ACTION_PEER_HANDSHAKE_ACK,
    ACTION_CHAT_MESSAGE,
    ACTION_PEER_DISCONNECT,
)

logger = logging.getLogger("p2p_chat.client")


class PeerClient:
    """
    P2P Chat Client that:
    1. Listens for incoming direct TCP chat connections from other peers.
    2. Maintains a signaling connection for online user discovery and endpoint lookup.
    3. Establishes and manages direct, encrypted/isolated P2P TCP connections with peers.
    """

    def __init__(
        self,
        username: str,
        signaling_ip: str,
        signaling_port: int,
        listen_host: str = "0.0.0.0",
        listen_port: int = 6001,
        advertised_ip: Optional[str] = None,
    ):
        self.username = username
        self.signaling_ip = signaling_ip
        self.signaling_port = signaling_port
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.advertised_ip = advertised_ip or get_lan_ip()

        # Signaling connection
        self.signaling_sock: Optional[socket.socket] = None
        self.signaling_connected = False

        # Peer listener socket
        self.listener_sock: Optional[socket.socket] = None
        self.is_running = False

        # Active direct P2P connections: username -> {"socket": socket.socket, "ip": str, "port": int}
        self.active_peers: Dict[str, Dict[str, Any]] = {}
        # socket -> username
        self.sock_to_peer: Dict[socket.socket, str] = {}
        self.peers_lock = threading.Lock()

        # Currently active chat partner
        self.active_chat_partner: Optional[str] = None

        # Synchronization for signaling request/responses (e.g. list_users, connect_req)
        self.pending_requests: Dict[str, threading.Event] = {}
        self.response_data: Dict[str, Any] = {}
        self.req_lock = threading.Lock()

        # Console output lock
        self.print_lock = threading.Lock()

    def safe_print(self, message: str, end: str = "\n") -> None:
        """Thread-safe terminal printing that restores the CLI prompt cleanly."""
        with self.print_lock:
            # Overwrite current line, print message, and restore prompt if running
            sys.stdout.write(f"\r\033[K{message}{end}")
            if self.is_running:
                prompt_partner = f" [{self.active_chat_partner}]" if self.active_chat_partner else ""
                sys.stdout.write(f"CLI{prompt_partner} > ")
            sys.stdout.flush()

    # -------------------------------------------------------------------------
    # 1. P2P Direct Listener (Incoming Peer Connections)
    # -------------------------------------------------------------------------
    def start_peer_listener(self) -> None:
        """Bind TCP socket to listen for incoming direct peer connections."""
        self.listener_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sys.platform != "win32":
            self.listener_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        else:
            try:
                self.listener_sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            except Exception:
                pass

        try:
            self.listener_sock.bind((self.listen_host, self.listen_port))
            self.listener_sock.listen(10)
        except OSError as e:
            self.safe_print(f"[-] Could not bind P2P listener on port {self.listen_port}: {e}")
            raise

        self.is_running = True
        t = threading.Thread(target=self._peer_accept_loop, daemon=True)
        t.start()
        logger.info("Direct P2P listener active on %s:%d", self.listen_host, self.listen_port)

    def _peer_accept_loop(self) -> None:
        """Accept incoming direct TCP connections from peers."""
        while self.is_running:
            try:
                peer_sock, peer_addr = self.listener_sock.accept()
                t = threading.Thread(
                    target=self._handle_incoming_peer,
                    args=(peer_sock, peer_addr),
                    daemon=True,
                )
                t.start()
            except OSError:
                break

    def _handle_incoming_peer(self, peer_sock: socket.socket, peer_addr: tuple) -> None:
        """Perform handshake with incoming peer and start direct message loop."""
        remote_user = None
        try:
            # First message must be peer handshake
            msg = recv_msg(peer_sock)
            if not msg or msg.get("type") != ACTION_PEER_HANDSHAKE:
                peer_sock.close()
                return

            remote_user = msg.get("username")
            if not remote_user:
                peer_sock.close()
                return

            # Acknowledge handshake directly to peer
            send_msg(peer_sock, {"type": ACTION_PEER_HANDSHAKE_ACK, "username": self.username})

            with self.peers_lock:
                # If an existing connection to this peer already exists, close the old one
                if remote_user in self.active_peers:
                    old_sock = self.active_peers[remote_user]["socket"]
                    try:
                        old_sock.close()
                    except Exception:
                        pass

                self.active_peers[remote_user] = {
                    "socket": peer_sock,
                    "ip": peer_addr[0],
                    "port": peer_addr[1],
                }
                self.sock_to_peer[peer_sock] = remote_user

                if not self.active_chat_partner:
                    self.active_chat_partner = remote_user

            self.safe_print(f"\n[+] Direct P2P connection established with '{remote_user}' ({peer_addr[0]}:{peer_addr[1]})")
            self._peer_message_loop(peer_sock, remote_user)

        except Exception as e:
            logger.debug("Error during incoming peer handshake with %s: %s", remote_user or peer_addr, e)
            self._cleanup_peer(peer_sock, remote_user)

    # -------------------------------------------------------------------------
    # 2. Signaling Server Connection & Discovery
    # -------------------------------------------------------------------------
    def connect_to_signaling_server(self) -> bool:
        """Establish connection to signaling server and register this client."""
        try:
            self.signaling_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.signaling_sock.settimeout(5.0)
            self.signaling_sock.connect((self.signaling_ip, self.signaling_port))
            self.signaling_sock.settimeout(None)
            self.signaling_connected = True
        except (socket.error, OSError) as e:
            self.safe_print(f"[-] Could not connect to signaling server at {self.signaling_ip}:{self.signaling_port}: {e}")
            if self.signaling_sock:
                self.signaling_sock.close()
            return False

        # Register username and listening port
        reg_payload = {
            "type": ACTION_REGISTER,
            "username": self.username,
            "listen_port": self.listen_port,
            "advertised_ip": self.advertised_ip,
        }
        try:
            send_msg(self.signaling_sock, reg_payload)
            resp = recv_msg(self.signaling_sock)
            if not resp or resp.get("type") != ACTION_REGISTER_ACK:
                err = resp.get("error") if resp else "No response from signaling server."
                self.safe_print(f"[-] Registration failed: {err}")
                self.signaling_sock.close()
                self.signaling_connected = False
                return False

            self.safe_print(f"[+] Registered as '{self.username}' on signaling server ({self.signaling_ip}:{self.signaling_port})")
            # Start background thread to listen for signaling server updates
            t = threading.Thread(target=self._signaling_listen_loop, daemon=True)
            t.start()
            return True

        except Exception as e:
            self.safe_print(f"[-] Error during signaling registration: {e}")
            if self.signaling_sock:
                self.signaling_sock.close()
            self.signaling_connected = False
            return False

    def _signaling_listen_loop(self) -> None:
        """Listen for discovery updates and incoming connection notifications from signaling server."""
        while self.is_running and self.signaling_connected:
            try:
                msg = recv_msg(self.signaling_sock)
                if msg is None:
                    # Server closed signaling connection
                    break

                action = msg.get("type")

                if action == ACTION_USER_LIST:
                    with self.req_lock:
                        self.response_data[ACTION_USER_LIST] = msg.get("users", [])
                        if ACTION_USER_LIST in self.pending_requests:
                            self.pending_requests[ACTION_USER_LIST].set()

                elif action == ACTION_PEER_INFO:
                    direction = msg.get("direction")
                    if direction == "incoming_notice":
                        peer_name = msg.get("peer_user")
                        self.safe_print(f"[*] Peer '{peer_name}' is attempting to connect directly to you...")
                    else:
                        with self.req_lock:
                            self.response_data[ACTION_PEER_INFO] = msg
                            if ACTION_PEER_INFO in self.pending_requests:
                                self.pending_requests[ACTION_PEER_INFO].set()

                elif action == ACTION_CONNECT_ERROR:
                    with self.req_lock:
                        self.response_data[ACTION_CONNECT_ERROR] = msg
                        if ACTION_CONNECT_REQ in self.pending_requests:
                            self.pending_requests[ACTION_CONNECT_REQ].set()

                elif action == ACTION_USER_DISCONNECTED:
                    left_user = msg.get("username")
                    self.safe_print(f"[*] Signaling Notice: User '{left_user}' has gone offline.")

            except Exception as e:
                logger.debug("Signaling listen error: %s", e)
                break

        self.signaling_connected = False
        if self.is_running:
            self.safe_print("\n[!] Connection to signaling server lost. Direct P2P chats remain active.")

    # -------------------------------------------------------------------------
    # 3. Direct Peer-to-Peer Connection Outbound
    # -------------------------------------------------------------------------
    def connect_to_peer(self, target_username: str) -> bool:
        """
        Look up target peer via signaling server, establish direct TCP connection,
        perform peer handshake, and start direct message loop.
        """
        if target_username == self.username:
            self.safe_print("[-] You cannot chat with yourself.")
            return False

        with self.peers_lock:
            if target_username in self.active_peers:
                self.active_chat_partner = target_username
                self.safe_print(f"[+] Already connected to '{target_username}'. Active chat set to '{target_username}'.")
                return True

        if not self.signaling_connected:
            self.safe_print("[-] Signaling server unavailable. Cannot discover peer network endpoint.")
            return False

        self.safe_print(f"[*] Discovering network endpoint for '{target_username}' from signaling server...")

        # Request peer connection info from signaling server
        req_event = threading.Event()
        with self.req_lock:
            self.pending_requests[ACTION_PEER_INFO] = req_event
            self.pending_requests[ACTION_CONNECT_REQ] = req_event
            self.response_data.pop(ACTION_PEER_INFO, None)
            self.response_data.pop(ACTION_CONNECT_ERROR, None)

        try:
            send_msg(self.signaling_sock, {"type": ACTION_CONNECT_REQ, "target_user": target_username})
        except Exception as e:
            self.safe_print(f"[-] Failed to send connect request to signaling server: {e}")
            return False

        # Wait for signaling server response (timeout 5s)
        if not req_event.wait(timeout=5.0):
            self.safe_print("[-] Connection request timed out waiting for signaling server.")
            return False

        with self.req_lock:
            err_info = self.response_data.get(ACTION_CONNECT_ERROR)
            peer_info = self.response_data.get(ACTION_PEER_INFO)

        if err_info:
            self.safe_print(f"[-] Could not connect to '{target_username}': {err_info.get('error')}")
            return False

        if not peer_info:
            self.safe_print(f"[-] No connection information received for '{target_username}'.")
            return False

        peer_ip = peer_info.get("peer_ip")
        peer_port = peer_info.get("peer_port")

        self.safe_print(f"[*] Establishing direct TCP connection to {peer_ip}:{peer_port}...")

        # Create direct peer socket
        peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_sock.settimeout(6.0)
        try:
            peer_sock.connect((peer_ip, peer_port))
            peer_sock.settimeout(None)

            # Transmit direct peer handshake
            send_msg(peer_sock, {"type": ACTION_PEER_HANDSHAKE, "username": self.username})
            ack = recv_msg(peer_sock)

            if not ack or ack.get("type") != ACTION_PEER_HANDSHAKE_ACK:
                self.safe_print("[-] Peer handshake failed.")
                peer_sock.close()
                return False

            with self.peers_lock:
                self.active_peers[target_username] = {
                    "socket": peer_sock,
                    "ip": peer_ip,
                    "port": peer_port,
                }
                self.sock_to_peer[peer_sock] = target_username
                self.active_chat_partner = target_username

            self.safe_print(f"[+] Direct P2P connection to '{target_username}' established! (Active chat set to '{target_username}')")

            # Start message receiving thread for this peer
            t = threading.Thread(
                target=self._peer_message_loop,
                args=(peer_sock, target_username),
                daemon=True,
            )
            t.start()
            return True

        except (socket.error, OSError) as e:
            self.safe_print(
                f"[-] Direct connection to {peer_ip}:{peer_port} failed: {e}\n"
                f"    (Ensure both devices are on the same Wi-Fi and port {peer_port} is not blocked by Windows Firewall)"
            )
            peer_sock.close()
            return False

    def _peer_message_loop(self, peer_sock: socket.socket, peer_user: str) -> None:
        """Receive chat messages directly from the peer over the peer TCP socket."""
        try:
            while self.is_running:
                msg = recv_msg(peer_sock)
                if msg is None:
                    # Peer closed direct connection
                    break

                action = msg.get("type")
                if action == ACTION_CHAT_MESSAGE:
                    sender = msg.get("from", peer_user)
                    text = msg.get("text", "")
                    timestamp = time.strftime("%H:%M:%S", time.localtime(msg.get("timestamp", time.time())))
                    # Display message directly
                    self.safe_print(f"\n[{timestamp}] [{sender}]: {text}")

                elif action == ACTION_PEER_DISCONNECT:
                    self.safe_print(f"\n[*] Peer '{peer_user}' closed the direct chat session.")
                    break

        except (ConnectionResetError, ConnectionAbortedError):
            pass
        except Exception as e:
            logger.debug("Peer message loop error for %s: %s", peer_user, e)
        finally:
            self._cleanup_peer(peer_sock, peer_user)

    def _cleanup_peer(self, peer_sock: socket.socket, peer_user: Optional[str]) -> None:
        """Close socket and clean up active peer record."""
        target_name = peer_user
        with self.peers_lock:
            if not target_name:
                target_name = self.sock_to_peer.get(peer_sock)
            if target_name and target_name in self.active_peers:
                del self.active_peers[target_name]
            if peer_sock in self.sock_to_peer:
                del self.sock_to_peer[peer_sock]

            if self.active_chat_partner == target_name:
                # Switch active chat partner to another connected peer if any, otherwise None
                self.active_chat_partner = next(iter(self.active_peers)) if self.active_peers else None

        try:
            peer_sock.close()
        except Exception:
            pass

        if target_name:
            self.safe_print(f"\n[!] Direct chat session with '{target_name}' disconnected.")

    # -------------------------------------------------------------------------
    # 4. Chat and CLI Actions
    # -------------------------------------------------------------------------
    def send_chat_message(self, target_user: str, text: str) -> bool:
        """Send a direct message to a peer over the direct P2P socket."""
        if not text.strip():
            return False

        with self.peers_lock:
            peer_info = self.active_peers.get(target_user)

        if not peer_info:
            # Not yet connected directly. Try establishing connection first
            self.safe_print(f"[*] Not connected to '{target_user}'. Attempting direct connection...")
            if not self.connect_to_peer(target_user):
                return False
            with self.peers_lock:
                peer_info = self.active_peers.get(target_user)
            if not peer_info:
                return False

        peer_sock = peer_info["socket"]
        payload = {
            "type": ACTION_CHAT_MESSAGE,
            "from": self.username,
            "to": target_user,
            "text": text,
            "timestamp": time.time(),
        }

        try:
            # Transmit directly to peer socket
            send_msg(peer_sock, payload)
            timestamp = time.strftime("%H:%M:%S")
            self.safe_print(f"[{timestamp}] [You -> {target_user}]: {text}")
            return True
        except (socket.error, OSError) as e:
            self.safe_print(f"[-] Failed to send message to '{target_user}': {e}")
            self._cleanup_peer(peer_sock, target_user)
            return False

    def list_online_users(self) -> None:
        """Request and print list of online users from the signaling server."""
        if not self.signaling_connected:
            self.safe_print("[-] Not connected to signaling server. Cannot retrieve online users.")
            return

        req_event = threading.Event()
        with self.req_lock:
            self.pending_requests[ACTION_USER_LIST] = req_event
            self.response_data.pop(ACTION_USER_LIST, None)

        try:
            send_msg(self.signaling_sock, {"type": ACTION_LIST_USERS})
        except Exception as e:
            self.safe_print(f"[-] Failed to query signaling server: {e}")
            return

        if not req_event.wait(timeout=4.0):
            self.safe_print("[-] Timed out waiting for user list from signaling server.")
            return

        with self.req_lock:
            users = self.response_data.get(ACTION_USER_LIST, [])

        self.safe_print("\n" + "=" * 45)
        self.safe_print("              ONLINE USERS")
        self.safe_print("=" * 45)
        if not users:
            self.safe_print("  No other users currently registered online.")
        else:
            for u in users:
                uname = u.get("username")
                uip = u.get("ip")
                uport = u.get("port")
                with self.peers_lock:
                    connected_status = " [DIRECT P2P CONNECTED]" if uname in self.active_peers else ""
                self.safe_print(f"  • {uname:<15} ({uip}:{uport}){connected_status}")
        self.safe_print("=" * 45 + "\n")

    def show_status(self) -> None:
        """Display detailed connection status."""
        sig_status = "CONNECTED" if self.signaling_connected else "DISCONNECTED"
        self.safe_print("\n" + "=" * 45)
        self.safe_print("           CONNECTION STATUS")
        self.safe_print("=" * 45)
        self.safe_print(f"  Username:            {self.username}")
        self.safe_print(f"  Local LAN IP:        {self.advertised_ip}")
        self.safe_print(f"  P2P Listen Port:     {self.listen_port}")
        self.safe_print(f"  Signaling Server:    {self.signaling_ip}:{self.signaling_port} ({sig_status})")
        self.safe_print(f"  Active Chat Partner: {self.active_chat_partner or 'None'}")

        with self.peers_lock:
            peers = list(self.active_peers.items())

        self.safe_print(f"  Direct P2P Sessions: {len(peers)}")
        for uname, info in peers:
            self.safe_print(f"    - {uname:<12} ({info['ip']}:{info['port']})")
        self.safe_print("=" * 45 + "\n")

    def show_help(self) -> None:
        """Display available CLI commands."""
        help_text = """
Commands:
  /users                 - List online users registered on signaling server
  /chat <username>       - Connect directly to peer and set as active chat partner
  /msg <username> <text> - Send a direct message to a specific peer
  /status                - Display connection status and active P2P sessions
  /close [username]      - Close direct chat session with a peer
  /help                  - Show this help message
  /exit                  - Disconnect all sessions and exit

Tip: Once connected to a peer via /chat <username>, any text entered without
a leading '/' command is sent directly to your active chat partner.
"""
        self.safe_print(help_text)

    def close_peer(self, target_user: Optional[str]) -> None:
        """Close direct connection with a peer."""
        target = target_user or self.active_chat_partner
        if not target:
            self.safe_print("[-] No peer specified and no active chat partner.")
            return

        with self.peers_lock:
            peer_info = self.active_peers.get(target)

        if not peer_info:
            self.safe_print(f"[-] Not directly connected to '{target}'.")
            return

        sock = peer_info["socket"]
        try:
            send_msg(sock, {"type": ACTION_PEER_DISCONNECT})
        except Exception:
            pass
        self._cleanup_peer(sock, target)
        self.safe_print(f"[+] Closed direct connection with '{target}'.")

    def disconnect_all(self) -> None:
        """Cleanly close all peer sockets and signaling socket."""
        self.is_running = False

        # Close all peer connections
        with self.peers_lock:
            for uname, info in list(self.active_peers.items()):
                try:
                    send_msg(info["socket"], {"type": ACTION_PEER_DISCONNECT})
                    info["socket"].close()
                except Exception:
                    pass
            self.active_peers.clear()
            self.sock_to_peer.clear()

        # Close peer listener
        if self.listener_sock:
            try:
                self.listener_sock.close()
            except Exception:
                pass

        # Close signaling connection
        if self.signaling_sock:
            try:
                self.signaling_sock.close()
            except Exception:
                pass
            self.signaling_connected = False


# -----------------------------------------------------------------------------
# Interactive CLI Loop
# -----------------------------------------------------------------------------
def run_cli():
    """Interactive Command Line Interface entry point."""
    config = AppConfig()
    config.configure_logging("p2p_chat.client")

    print("=" * 45)
    print("       PYTHON P2P CLI CHAT")
    print("=" * 45)

    # 1. Username resolution: CLI argument > environment variable > interactive prompt
    username = ""
    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        username = sys.argv[1].strip()
    elif config.client_username:
        username = config.client_username

    while not username:
        try:
            val = input("Enter username: ").strip()
            if len(val) >= 2 and val.replace("_", "").replace("-", "").isalnum():
                username = val
            else:
                print("Username must be at least 2 alphanumeric characters (dashes and underscores allowed).")
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            sys.exit(0)

    # 2. Server IP resolution: CLI argument > environment variable > interactive prompt
    server_ip = config.signaling_server_ip
    if len(sys.argv) > 2:
        try:
            validate_ip(sys.argv[2], "Signaling Server IP", allow_all_interfaces=False)
            server_ip = sys.argv[2].strip()
        except ValueError as e:
            print(f"Warning: {e}. Falling back to configured IP: {server_ip}")
    elif "SIGNALING_SERVER_IP" not in os.environ:
        try:
            prompt_str = f"Enter server LAN IP [{server_ip}]: "
            val = input(prompt_str).strip()
            if val:
                try:
                    validate_ip(val, "Signaling Server IP", allow_all_interfaces=False)
                    server_ip = val
                except ValueError as e:
                    print(f"Warning: {e}. Using {server_ip}")
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            sys.exit(0)

    # 3. P2P Listen Port resolution: CLI argument > environment variable > interactive prompt
    listen_port = config.client_listen_port
    if len(sys.argv) > 3:
        try:
            listen_port = validate_port(sys.argv[3], "P2P Listen Port")
        except ValueError as e:
            print(f"Warning: {e}. Using {listen_port}")
    elif "CLIENT_LISTEN_PORT" not in os.environ:
        try:
            port_input = input(f"Enter P2P listen port [{listen_port}]: ").strip()
            if port_input:
                try:
                    listen_port = validate_port(port_input, "P2P Listen Port")
                except ValueError as e:
                    print(f"Warning: {e}. Using {listen_port}")
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            sys.exit(0)

    # Try binding listener port; if occupied, allow user to increment
    client = None
    bind_attempts = 0
    cur_port = listen_port

    while bind_attempts < 5:
        try:
            client = PeerClient(
                username=username,
                signaling_ip=server_ip,
                signaling_port=config.server_port,
                listen_host=config.client_listen_host,
                listen_port=cur_port,
            )
            client.start_peer_listener()
            break
        except OSError:
            bind_attempts += 1
            cur_port += 1
            print(f"[-] Port {cur_port - 1} is already in use. Trying port {cur_port}...")

    if not client or not client.is_running:
        print(f"[-] Could not bind listening socket. Please configure an available port.")
        sys.exit(1)

    # Connect to signaling server
    if not client.connect_to_signaling_server():
        print("[-] Could not connect to signaling server.")
        print("[*] Note: You can still listen for direct incoming connections, but discovery is disabled.")
    else:
        print("[+] Discovery service connected. Discovering online peers...")
        client.list_online_users()

    client.show_help()

    # Main CLI loop
    try:
        while client.is_running:
            partner_tag = f" [{client.active_chat_partner}]" if client.active_chat_partner else ""
            try:
                line = input(f"CLI{partner_tag} > ").strip()
            except EOFError:
                break

            if not line:
                continue

            if line.startswith("/"):
                parts = line.split(maxsplit=2)
                cmd = parts[0].lower()

                if cmd == "/users":
                    client.list_online_users()

                elif cmd == "/chat":
                    if len(parts) < 2:
                        print("Usage: /chat <username>")
                    else:
                        target = parts[1].strip()
                        client.connect_to_peer(target)

                elif cmd == "/msg":
                    if len(parts) < 3:
                        print("Usage: /msg <username> <message text>")
                    else:
                        target = parts[1].strip()
                        text = parts[2].strip()
                        client.send_chat_message(target, text)

                elif cmd == "/status":
                    client.show_status()

                elif cmd == "/close":
                    target = parts[1].strip() if len(parts) > 1 else None
                    client.close_peer(target)

                elif cmd == "/help":
                    client.show_help()

                elif cmd == "/exit":
                    print("[*] Exiting chat application...")
                    break

                else:
                    print(f"[-] Unknown command '{cmd}'. Type /help for available commands.")

            else:
                # Text entered directly without a slash command
                if client.active_chat_partner:
                    client.send_chat_message(client.active_chat_partner, line)
                else:
                    print("[-] No active chat partner selected. Use /chat <username> or /msg <username> <text>")

    except KeyboardInterrupt:
        print("\n[*] Exiting chat application...")
    finally:
        if client:
            client.disconnect_all()
        print("[*] Goodbye!")


if __name__ == "__main__":
    run_cli()
