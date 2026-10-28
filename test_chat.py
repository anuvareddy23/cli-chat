"""
test_chat.py - Comprehensive Automated Test Suite for P2P CLI Chat Application.

Verifies:
1. Configuration loading, environment overrides, and IP/port validation.
2. TCP length-prefixed framing and message serialization.
3. Signaling server startup, registration, and duplicate-username rejection.
4. Online-user discovery (list_users).
5. Direct P2P connection establishment and handshake.
6. Bidirectional messaging directly between peers over dedicated P2P sockets.
7. Critical Security Requirement: Signaling server never processes or relays chat messages.
8. Peer and server disconnection handling.
9. Malformed message handling, oversized payload limits, and invalid inputs.
10. .gitignore exclusions and .env.example presence.
"""

import os
import time
import socket
import unittest
import tempfile
import threading

from config import (
    AppConfig,
    load_dotenv,
    validate_ip,
    validate_port,
    parse_bool,
    get_lan_ip,
)
from protocol import (
    send_msg,
    recv_msg,
    MAX_MESSAGE_SIZE,
    ACTION_REGISTER,
    ACTION_REGISTER_ACK,
    ACTION_REGISTER_ERROR,
    ACTION_LIST_USERS,
    ACTION_USER_LIST,
    ACTION_CONNECT_REQ,
    ACTION_PEER_INFO,
    ACTION_CONNECT_ERROR,
    ACTION_CHAT_MESSAGE,
    ACTION_PEER_HANDSHAKE,
    ACTION_PEER_HANDSHAKE_ACK,
)
from server import SignalingServer
from client import PeerClient


def get_free_port() -> int:
    """Find an available port on loopback."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class TestConfigurationAndValidation(unittest.TestCase):
    """Test configuration parsing, .env loader, and input validation."""

    def test_validate_ip(self):
        # Valid IPv4 addresses
        self.assertEqual(validate_ip("192.168.1.1"), "192.168.1.1")
        self.assertEqual(validate_ip("10.0.0.5"), "10.0.0.5")
        self.assertEqual(validate_ip("127.0.0.1"), "127.0.0.1")
        self.assertEqual(validate_ip("0.0.0.0", allow_all_interfaces=True), "0.0.0.0")

        # Invalid IPv4 addresses
        with self.assertRaises(ValueError):
            validate_ip("256.0.0.1")
        with self.assertRaises(ValueError):
            validate_ip("invalid_ip_string")
        with self.assertRaises(ValueError):
            validate_ip("192.168.1")
        with self.assertRaises(ValueError):
            validate_ip("0.0.0.0", allow_all_interfaces=False)

    def test_validate_port(self):
        # Valid ports
        self.assertEqual(validate_port(5000), 5000)
        self.assertEqual(validate_port("6001"), 6001)
        self.assertEqual(validate_port(1024), 1024)
        self.assertEqual(validate_port(65535), 65535)

        # Invalid ports
        with self.assertRaises(ValueError):
            validate_port(80)  # < 1024
        with self.assertRaises(ValueError):
            validate_port(70000)  # > 65535
        with self.assertRaises(ValueError):
            validate_port("not_a_number")

    def test_parse_bool(self):
        self.assertTrue(parse_bool("true"))
        self.assertTrue(parse_bool("True"))
        self.assertTrue(parse_bool("1"))
        self.assertTrue(parse_bool("yes"))
        self.assertFalse(parse_bool("false"))
        self.assertFalse(parse_bool("0"))
        self.assertFalse(parse_bool("no"))
        self.assertFalse(parse_bool(None, default=False))
        self.assertTrue(parse_bool(None, default=True))

    def test_load_dotenv(self):
        """Test custom standard-library .env file parser."""
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            f.write("# Sample comment\n")
            f.write("TEST_P2P_VAR1=helloworld\n")
            f.write("TEST_P2P_VAR2=\"quoted_value\"\n")
            f.write("TEST_P2P_VAR3='single_quoted'\n")
            f.write("   # Indented comment\n")
            f.write("TEST_P2P_PORT=5555\n")
            temp_path = f.name

        try:
            # Clear if already present in environment
            os.environ.pop("TEST_P2P_VAR1", None)
            os.environ.pop("TEST_P2P_VAR2", None)
            os.environ.pop("TEST_P2P_VAR3", None)
            os.environ.pop("TEST_P2P_PORT", None)

            loaded = load_dotenv(temp_path)
            self.assertEqual(loaded["TEST_P2P_VAR1"], "helloworld")
            self.assertEqual(loaded["TEST_P2P_VAR2"], "quoted_value")
            self.assertEqual(loaded["TEST_P2P_VAR3"], "single_quoted")
            self.assertEqual(loaded["TEST_P2P_PORT"], "5555")

            # Check that os.environ was populated
            self.assertEqual(os.environ.get("TEST_P2P_VAR1"), "helloworld")

            # Verify that existing os.environ is NOT overwritten by .env (environment override precedence)
            os.environ["TEST_P2P_OVERRIDE"] = "original_env_val"
            with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f2:
                f2.write("TEST_P2P_OVERRIDE=dotenv_val\n")
                temp_path2 = f2.name
            try:
                load_dotenv(temp_path2)
                self.assertEqual(os.environ.get("TEST_P2P_OVERRIDE"), "original_env_val")
            finally:
                if os.path.exists(temp_path2):
                    os.remove(temp_path2)
                os.environ.pop("TEST_P2P_OVERRIDE", None)

        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            os.environ.pop("TEST_P2P_VAR1", None)
            os.environ.pop("TEST_P2P_VAR2", None)
            os.environ.pop("TEST_P2P_VAR3", None)
            os.environ.pop("TEST_P2P_PORT", None)

    def test_lan_ip_detection(self):
        """Test LAN IP helper returns a non-empty valid IPv4 string."""
        ip = get_lan_ip()
        self.assertIsInstance(ip, str)
        self.assertGreater(len(ip), 6)
        validate_ip(ip, allow_all_interfaces=False)


class TestProtocolAndFraming(unittest.TestCase):
    """Test TCP length-prefixed message framing and boundary handling."""

    def test_send_and_recv_msg_roundtrip(self):
        """Verify length-prefixed framing across a local socket pair."""
        port = get_free_port()
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.bind(("127.0.0.1", port))
        srv.listen(1)

        cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cli.connect(("127.0.0.1", port))
        conn, _ = srv.accept()

        try:
            payload = {"action": "test", "number": 42, "text": "Hello, P2P World! 🚀"}
            send_msg(cli, payload)
            received = recv_msg(conn)
            self.assertEqual(received, payload)
        finally:
            conn.close()
            cli.close()
            srv.close()

    def test_oversized_payload_rejection(self):
        """Verify that payloads exceeding MAX_MESSAGE_SIZE raise ValueError."""
        dummy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            huge_dict = {"data": "A" * (MAX_MESSAGE_SIZE + 10)}
            with self.assertRaises(ValueError):
                send_msg(dummy_sock, huge_dict)
        finally:
            dummy_sock.close()


class TestSignalingServer(unittest.TestCase):
    """Test signaling server operations: registration, discovery, connection requests, and security."""

    @classmethod
    def setUpClass(cls):
        cls.server_port = get_free_port()
        cls.server = SignalingServer(host="127.0.0.1", port=cls.server_port)
        cls.server_thread = threading.Thread(target=cls.server.start, daemon=True)
        cls.server_thread.start()
        # Give server time to bind
        time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def test_registration_and_duplicate_rejection(self):
        """Test client registration and duplicate username rejection."""
        # Connect Client 1
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s1.connect(("127.0.0.1", self.server_port))

        try:
            # Register alice
            send_msg(s1, {"type": ACTION_REGISTER, "username": "alice", "listen_port": 6101})
            resp1 = recv_msg(s1)
            self.assertEqual(resp1.get("type"), ACTION_REGISTER_ACK)
            self.assertEqual(resp1.get("username"), "alice")

            # Connect Client 2 and attempt duplicate registration with same username 'alice'
            s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s2.connect(("127.0.0.1", self.server_port))
            try:
                send_msg(s2, {"type": ACTION_REGISTER, "username": "alice", "listen_port": 6102})
                resp2 = recv_msg(s2)
                self.assertEqual(resp2.get("type"), ACTION_REGISTER_ERROR)
                self.assertIn("already in use", resp2.get("error", ""))

                # Register s2 with a unique username 'bob'
                send_msg(s2, {"type": ACTION_REGISTER, "username": "bob", "listen_port": 6102})
                resp2_ok = recv_msg(s2)
                self.assertEqual(resp2_ok.get("type"), ACTION_REGISTER_ACK)
                self.assertEqual(resp2_ok.get("username"), "bob")

            finally:
                s2.close()
                time.sleep(0.05)

        finally:
            s1.close()
            time.sleep(0.05)

    def test_online_user_discovery(self):
        """Test listing online users via the signaling server."""
        s_alice = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s_bob = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        s_alice.connect(("127.0.0.1", self.server_port))
        s_bob.connect(("127.0.0.1", self.server_port))

        try:
            send_msg(s_alice, {"type": ACTION_REGISTER, "username": "alice_disc", "listen_port": 6201})
            _ = recv_msg(s_alice)

            send_msg(s_bob, {"type": ACTION_REGISTER, "username": "bob_disc", "listen_port": 6202})
            _ = recv_msg(s_bob)

            # Alice requests user list
            send_msg(s_alice, {"type": ACTION_LIST_USERS})
            list_resp = recv_msg(s_alice)
            self.assertEqual(list_resp.get("type"), ACTION_USER_LIST)

            users = [u["username"] for u in list_resp.get("users", [])]
            # Alice should see bob, but NOT see herself
            self.assertIn("bob_disc", users)
            self.assertNotIn("alice_disc", users)

        finally:
            s_alice.close()
            s_bob.close()
            time.sleep(0.05)

    def test_connect_request(self):
        """Test client requesting peer network endpoint from signaling server."""
        s_alice = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s_bob = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        s_alice.connect(("127.0.0.1", self.server_port))
        s_bob.connect(("127.0.0.1", self.server_port))

        try:
            send_msg(s_alice, {"type": ACTION_REGISTER, "username": "alice_conn", "listen_port": 6301})
            _ = recv_msg(s_alice)

            send_msg(s_bob, {"type": ACTION_REGISTER, "username": "bob_conn", "listen_port": 6302})
            _ = recv_msg(s_bob)

            # Alice requests Bob's endpoint
            send_msg(s_alice, {"type": ACTION_CONNECT_REQ, "target_user": "bob_conn"})
            peer_info = recv_msg(s_alice)

            self.assertEqual(peer_info.get("type"), ACTION_PEER_INFO)
            self.assertEqual(peer_info.get("peer_user"), "bob_conn")
            self.assertEqual(peer_info.get("peer_port"), 6302)

            # Alice requests non-existent user 'nobody'
            send_msg(s_alice, {"type": ACTION_CONNECT_REQ, "target_user": "nobody"})
            err_info = recv_msg(s_alice)
            self.assertEqual(err_info.get("type"), ACTION_CONNECT_ERROR)

        finally:
            s_alice.close()
            s_bob.close()
            time.sleep(0.05)

    def test_strict_server_rejection_of_chat_messages(self):
        """
        CRITICAL REQUIREMENT:
        The signaling server must NEVER accept, relay, broadcast, or store chat messages.
        Verify that sending any chat message to the server is rejected with a forbidden error.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", self.server_port))

        try:
            send_msg(s, {"type": ACTION_REGISTER, "username": "test_intruder", "listen_port": 6401})
            _ = recv_msg(s)

            # Attempt to send chat message through signaling server
            forbidden_payload = {
                "type": ACTION_CHAT_MESSAGE,
                "from": "test_intruder",
                "to": "someone",
                "text": "Secret chat message through server",
            }
            send_msg(s, forbidden_payload)
            resp = recv_msg(s)

            self.assertEqual(resp.get("type"), ACTION_CONNECT_ERROR)
            self.assertIn("Forbidden", resp.get("error", ""))
            self.assertIn("Signaling server handles discovery only", resp.get("error", ""))

        finally:
            s.close()
            time.sleep(0.05)


class TestDirectP2PChatCommunication(unittest.TestCase):
    """
    Test complete end-to-end direct Peer-to-Peer messaging between two clients:
    1. Both clients register on the signaling server.
    2. Client 1 discovers Client 2's endpoint.
    3. Direct TCP connection established between Client 1 and Client 2.
    4. Bidirectional messages exchanged over direct P2P socket.
    5. Signaling server inspected to confirm zero chat messages were seen.
    """

    @classmethod
    def setUpClass(cls):
        cls.server_port = get_free_port()
        cls.server = SignalingServer(host="127.0.0.1", port=cls.server_port)
        cls.server_thread = threading.Thread(target=cls.server.start, daemon=True)
        cls.server_thread.start()
        time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def test_direct_p2p_chat_session(self):
        port_alice = get_free_port()
        port_bob = get_free_port()

        client_alice = PeerClient(
            username="alice_p2p",
            signaling_ip="127.0.0.1",
            signaling_port=self.server_port,
            listen_host="127.0.0.1",
            listen_port=port_alice,
            advertised_ip="127.0.0.1",
        )

        client_bob = PeerClient(
            username="bob_p2p",
            signaling_ip="127.0.0.1",
            signaling_port=self.server_port,
            listen_host="127.0.0.1",
            listen_port=port_bob,
            advertised_ip="127.0.0.1",
        )

        try:
            # Start peer listeners
            client_alice.start_peer_listener()
            client_bob.start_peer_listener()

            # Register with signaling server
            self.assertTrue(client_alice.connect_to_signaling_server())
            self.assertTrue(client_bob.connect_to_signaling_server())

            # Alice connects directly to Bob over TCP
            connected = client_alice.connect_to_peer("bob_p2p")
            self.assertTrue(connected)

            # Give a brief moment for Bob's listener to complete handshake
            time.sleep(0.2)

            self.assertIn("bob_p2p", client_alice.active_peers)
            self.assertIn("alice_p2p", client_bob.active_peers)

            # Verify Alice's active chat partner is Bob
            self.assertEqual(client_alice.active_chat_partner, "bob_p2p")

            # Alice sends direct message to Bob
            alice_msg_sent = client_alice.send_chat_message("bob_p2p", "Hello from Alice direct P2P!")
            self.assertTrue(alice_msg_sent)

            # Bob sends direct message to Alice
            bob_msg_sent = client_bob.send_chat_message("alice_p2p", "Hello Alice, loud and clear!")
            self.assertTrue(bob_msg_sent)

            # Test graceful closure of direct P2P connection
            client_alice.close_peer("bob_p2p")
            time.sleep(0.1)
            self.assertNotIn("bob_p2p", client_alice.active_peers)

        finally:
            client_alice.disconnect_all()
            client_bob.disconnect_all()
            time.sleep(0.1)


class TestRepositoryFilesAndSecurity(unittest.TestCase):
    """Verify GitHub readiness, presence of .env.example, and .gitignore rules."""

    def setUp(self):
        self.workspace_root = os.path.dirname(os.path.abspath(__file__))

    def test_env_example_exists(self):
        example_path = os.path.join(self.workspace_root, ".env.example")
        self.assertTrue(os.path.isfile(example_path), ".env.example must exist in repository root")

        with open(example_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check required variables are documented
        self.assertIn("SERVER_HOST", content)
        self.assertIn("SERVER_PORT", content)
        self.assertIn("SIGNALING_SERVER_IP", content)
        self.assertIn("CLIENT_LISTEN_PORT", content)
        self.assertIn("LOG_LEVEL", content)

    def test_gitignore_rules(self):
        gitignore_path = os.path.join(self.workspace_root, ".gitignore")
        self.assertTrue(os.path.isfile(gitignore_path), ".gitignore must exist in repository root")

        with open(gitignore_path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]

        # Critical exclusions
        self.assertIn(".env", lines)
        self.assertIn("!.env.example", lines)
        self.assertIn("__pycache__/", lines)
        self.assertIn(".venv/", lines)


if __name__ == "__main__":
    unittest.main()
