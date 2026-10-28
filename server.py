"""
server.py - P2P Signaling and Discovery Server.

Facilitates discovery and connection-metadata exchange between peers.
STRICT RULE: This server never receives, relays, broadcasts, or stores chat messages.
All chat communication is strictly direct peer-to-peer over separate TCP sockets.
"""

import sys
import time
import socket
import logging
import threading
import re
from typing import Dict, Any, Optional

from config import AppConfig, get_lan_ip
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
    ACTION_HEARTBEAT,
    ACTION_HEARTBEAT_ACK,
    ALLOWED_SIGNALING_ACTIONS,
)

logger = logging.getLogger("p2p_chat.server")

# Username validation pattern: 2 to 24 characters, letters, numbers, underscores, dashes
USERNAME_REGEX = re.compile(r"^[A-Za-z0-9_-]{2,24}$")


class SignalingServer:
    """
    Lightweight, multi-threaded TCP signaling server for peer discovery.
    Coordinates peer registration and connection requests only.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 5000):
        self.host = host
        self.port = port
        self.server_sock: Optional[socket.socket] = None
        self.is_running = False

        # Registry of connected clients
        # username -> {"ip": str, "listen_port": int, "socket": socket.socket, "connected_at": float}
        self.clients: Dict[str, Dict[str, Any]] = {}
        # socket -> username
        self.sock_to_username: Dict[socket.socket, str] = {}
        self.lock = threading.Lock()

    def start(self) -> None:
        """Bind server socket and begin listening for client connections."""
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sys.platform != "win32":
            self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        else:
            try:
                self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            except Exception:
                pass

        try:
            self.server_sock.bind((self.host, self.port))
            self.server_sock.listen(50)
            self.is_running = True
        except OSError as e:
            logger.error("Failed to bind signaling server on %s:%d: %s", self.host, self.port, e)
            if self.server_sock:
                self.server_sock.close()
            raise

        lan_ip = get_lan_ip()
        print("=" * 60)
        print("      P2P CHAT SIGNALING SERVER (DISCOVERY ONLY)")
        print("=" * 60)
        print(f"[*] Bound on:         {self.host}:{self.port}")
        print(f"[*] Local LAN IP:     {lan_ip}")
        print(f"[*] Port:             {self.port}")
        print(f"[*] Other devices on the Wi-Fi should connect to: {lan_ip}")
        print("[*] Note: Chat messages are NEVER relayed through this server.")
        print("=" * 60)
        print("[*] Waiting for client connections... Press Ctrl+C to stop.\n")

        self._accept_loop()

    def _accept_loop(self) -> None:
        """Main loop accepting incoming client connections."""
        while self.is_running:
            try:
                client_sock, client_addr = self.server_sock.accept()
                logger.info("Accepted signaling connection from %s:%d", client_addr[0], client_addr[1])
                t = threading.Thread(
                    target=self._handle_client,
                    args=(client_sock, client_addr),
                    daemon=True,
                )
                t.start()
            except OSError:
                # Occurs when socket is closed during shutdown
                break

    def _handle_client(self, client_sock: socket.socket, client_addr: tuple) -> None:
        """Handle signaling requests from a single client connection."""
        client_ip = client_addr[0]
        registered_user: Optional[str] = None

        try:
            while self.is_running:
                msg = recv_msg(client_sock)
                if msg is None:
                    # Client closed signaling connection
                    break

                action = msg.get("type")

                # STRICT SECURITY CHECK: Reject chat messages or unauthorized action types
                if action not in ALLOWED_SIGNALING_ACTIONS:
                    logger.warning(
                        "Client %s attempted invalid or forbidden action: %s",
                        registered_user or client_ip,
                        action,
                    )
                    send_msg(
                        client_sock,
                        {
                            "type": ACTION_CONNECT_ERROR,
                            "error": "Forbidden: Signaling server handles discovery only. Chat messages must be direct P2P.",
                        },
                    )
                    continue

                if action == ACTION_REGISTER:
                    registered_user = self._handle_register(client_sock, client_ip, msg)
                elif action == ACTION_LIST_USERS:
                    self._handle_list_users(client_sock, registered_user)
                elif action == ACTION_CONNECT_REQ:
                    self._handle_connect_req(client_sock, registered_user, msg)
                elif action == ACTION_HEARTBEAT:
                    send_msg(client_sock, {"type": ACTION_HEARTBEAT_ACK, "timestamp": time.time()})

        except (ConnectionResetError, ConnectionAbortedError) as e:
            logger.debug("Client connection reset: %s (%s)", registered_user or client_ip, e)
        except Exception as e:
            logger.error("Error handling client %s: %s", registered_user or client_ip, e, exc_info=True)
        finally:
            self._cleanup_client(client_sock, registered_user)

    def _handle_register(self, client_sock: socket.socket, client_ip: str, msg: Dict[str, Any]) -> Optional[str]:
        """Handle client registration with unique username and listening port."""
        username = str(msg.get("username", "")).strip()
        listen_port = msg.get("listen_port")

        if not USERNAME_REGEX.match(username):
            send_msg(
                client_sock,
                {
                    "type": ACTION_REGISTER_ERROR,
                    "error": "Invalid username. Must be 2-24 alphanumeric characters, dashes, or underscores.",
                },
            )
            return None

        try:
            listen_port = int(listen_port)
            if not (1024 <= listen_port <= 65535):
                raise ValueError()
        except (TypeError, ValueError):
            send_msg(
                client_sock,
                {
                    "type": ACTION_REGISTER_ERROR,
                    "error": "Invalid listen_port. Must be an integer between 1024 and 65535.",
                },
            )
            return None

        # Allow client to suggest its own LAN IP if it knows it, but default to the socket's peer IP
        # If client connects via 127.0.0.1 (local machine testing), respect client's advertised IP if provided
        advertised_ip = msg.get("advertised_ip")
        effective_ip = client_ip
        if client_ip in ("127.0.0.1", "::1") and advertised_ip and advertised_ip != "0.0.0.0":
            effective_ip = advertised_ip

        with self.lock:
            if username in self.clients:
                send_msg(
                    client_sock,
                    {
                        "type": ACTION_REGISTER_ERROR,
                        "error": f"Username '{username}' is already in use by another user.",
                    },
                )
                return None

            self.clients[username] = {
                "ip": effective_ip,
                "listen_port": listen_port,
                "socket": client_sock,
                "connected_at": time.time(),
            }
            self.sock_to_username[client_sock] = username

        logger.info("Registered user '%s' from %s (P2P listen port: %d)", username, effective_ip, listen_port)
        send_msg(
            client_sock,
            {
                "type": ACTION_REGISTER_ACK,
                "username": username,
                "registered_ip": effective_ip,
                "listen_port": listen_port,
            },
        )
        return username

    def _handle_list_users(self, client_sock: socket.socket, requester: Optional[str]) -> None:
        """Send list of all other online users with their connection endpoints."""
        with self.lock:
            user_list = [
                {
                    "username": uname,
                    "ip": info["ip"],
                    "port": info["listen_port"],
                }
                for uname, info in self.clients.items()
                if uname != requester
            ]

        send_msg(client_sock, {"type": ACTION_USER_LIST, "users": user_list})

    def _handle_connect_req(self, client_sock: socket.socket, requester: Optional[str], msg: Dict[str, Any]) -> None:
        """Handle request to obtain target peer's IP and listening port for direct connection."""
        if not requester:
            send_msg(
                client_sock,
                {"type": ACTION_CONNECT_ERROR, "error": "Must be registered to request connection."},
            )
            return

        target_user = str(msg.get("target_user", "")).strip()

        with self.lock:
            target_info = self.clients.get(target_user)
            requester_info = self.clients.get(requester)

        if not target_info:
            send_msg(
                client_sock,
                {
                    "type": ACTION_CONNECT_ERROR,
                    "target_user": target_user,
                    "error": f"User '{target_user}' is not online or not registered.",
                },
            )
            return

        # Send peer connection metadata back to requester
        send_msg(
            client_sock,
            {
                "type": ACTION_PEER_INFO,
                "peer_user": target_user,
                "peer_ip": target_info["ip"],
                "peer_port": target_info["listen_port"],
            },
        )

        # Notify target peer that requester is attempting to connect
        if requester_info:
            target_sock = target_info.get("socket")
            if target_sock:
                try:
                    send_msg(
                        target_sock,
                        {
                            "type": ACTION_PEER_INFO,
                            "peer_user": requester,
                            "peer_ip": requester_info["ip"],
                            "peer_port": requester_info["listen_port"],
                            "direction": "incoming_notice",
                        },
                    )
                except Exception as e:
                    logger.debug("Could not send incoming notice to %s: %s", target_user, e)

    def _cleanup_client(self, client_sock: socket.socket, username: Optional[str]) -> None:
        """Remove client from registry, notify remaining peers, and close socket."""
        disconnected_user = username
        with self.lock:
            if not disconnected_user:
                disconnected_user = self.sock_to_username.get(client_sock)

            if disconnected_user and disconnected_user in self.clients:
                del self.clients[disconnected_user]
            if client_sock in self.sock_to_username:
                del self.sock_to_username[client_sock]

            remaining_socks = [info["socket"] for info in self.clients.values()]

        try:
            client_sock.close()
        except Exception:
            pass

        if disconnected_user:
            logger.info("User '%s' disconnected from signaling server", disconnected_user)
            # Notify remaining clients
            disconnect_msg = {
                "type": ACTION_USER_DISCONNECTED,
                "username": disconnected_user,
            }
            for s in remaining_socks:
                try:
                    send_msg(s, disconnect_msg)
                except Exception:
                    pass

    def stop(self) -> None:
        """Gracefully stop server and close all active sockets."""
        self.is_running = False
        with self.lock:
            for info in self.clients.values():
                try:
                    info["socket"].close()
                except Exception:
                    pass
            self.clients.clear()
            self.sock_to_username.clear()

        if self.server_sock:
            try:
                self.server_sock.close()
            except Exception:
                pass
        logger.info("Signaling server stopped")


def main():
    """Command-line entry point for signaling server."""
    config = AppConfig()
    config.configure_logging("p2p_chat.server")

    # Override with command-line arguments if provided: python server.py [host] [port]
    host = config.server_host
    port = config.server_port

    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        try:
            port = int(sys.argv[2])
        except ValueError:
            print(f"Error: Invalid port argument '{sys.argv[2]}'")
            sys.exit(1)

    server = SignalingServer(host=host, port=port)
    try:
        server.start()
    except KeyboardInterrupt:
        print("\n[*] Shutting down signaling server...")
    finally:
        server.stop()
        print("[*] Server shutdown complete.")


if __name__ == "__main__":
    main()
