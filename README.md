# Python CLI-Based Peer-to-Peer (P2P) Chat Application

A high-performance, robust, and lightweight Command-Line Interface (CLI) chat application built with pure Python sockets and multi-threading. Designed for two devices connected to the same local Wi-Fi / LAN network to communicate directly peer-to-peer without relying on a central message relay.

---

## 1. Architectural Overview

The application is structured around a **Decentralized Direct Messaging** model with a lightweight **Signaling Discovery Server**:

```text
               +----------------------------------+
               |     Signaling Server             |
               |     (server.py)                  |
               |  - Discovery & Registry only     |
               |  - NEVER relays chat messages    |
               +----------------------------------+
                         /              \
         Registration & /                \ Registration &
      Endpoint Discovery/                  \ Endpoint Discovery
                       /                    \
                      v                      v
        +--------------------+        +--------------------+
        |      Client 1      |<======>|      Client 2      |
        |     (client.py)    | Direct |     (client.py)    |
        |                    |  TCP   |                    |
        +--------------------+ Stream +--------------------+
              (Device 1)                    (Device 2)
                   \                            /
                    ============================
                    Direct Peer-to-Peer Chat
```

### Core Architecture Principles:
1. **Signaling Server (`server.py`)**:
   - Maintains an in-memory directory of online users, their LAN IPv4 addresses, and their P2P listening ports.
   - Dispatches connection control metadata only (`register`, `list_users`, `connect_req`, `peer_info`).
   - **Zero-Relay Guarantee**: The signaling server **never** receives, inspects, relays, broadcasts, or stores chat messages. All message handling contains strict server-side validation rejecting any chat payloads.
2. **Direct Peer-to-Peer Messaging (`client.py`)**:
   - Each client acts simultaneously as a **TCP listener** (for incoming peer connections) and a **TCP client** (for outbound peer connections).
   - Once Alice discovers Bob's IP and port via the signaling server, Alice establishes a direct, independent TCP socket directly to Bob (`alice_ip:port <---> bob_ip:port`).
   - All subsequent messages travel exclusively across this direct P2P socket.
3. **TCP Message Framing (`protocol.py`)**:
   - TCP is a streaming byte protocol without built-in message boundaries.
   - To eliminate fragmentation and concatenated buffer issues, every message uses a **4-byte big-endian binary length header** followed by UTF-8 encoded JSON.
   - Enforces a 64 KB maximum payload limit to protect against memory exhaustion attacks.

---

## 2. Project Structure

```text
cli-chat/
├── client.py          # Terminal P2P chat client with interactive command loop
├── server.py          # Lightweight signaling and discovery server
├── protocol.py        # Length-prefixed framing and JSON protocol definitions
├── config.py          # Zero-dependency .env loader, IP/port validation, LAN discovery
├── test_chat.py       # Automated unit & integration test suite (14 test cases)
├── requirements.txt   # Python dependency documentation (Zero external dependencies)
├── .env.example       # Template environment variable configuration
├── .gitignore         # Git ignore specification (safely excludes local .env)
└── README.md          # Comprehensive technical and setup documentation
```

---

## 3. Prerequisites & Virtual Environment Setup

The application is written strictly using the **Python Standard Library** (`socket`, `threading`, `json`, `struct`, `os`, `sys`, `logging`, `ipaddress`, `unittest`). No third-party packages are required.

- **Python Version**: Python 3.8+ (Tested on Python 3.13)
- **Supported Platforms**: Windows 10/11, Linux, macOS

### Setting Up a Virtual Environment (Windows PowerShell)

Open Windows PowerShell in the project directory:

```powershell
# 1. Create a virtual environment named .venv
python -m venv .venv

# 2. If PowerShell script execution is restricted, temporarily enable it for your process:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process

# 3. Activate the virtual environment
.\.venv\Scripts\Activate.ps1

# 4. Verify Python environment
python --version
```

---

## 4. Configuration & Environment Variables

Configuration is loaded using a built-in standard-library `.env` parser with fallback to operating system environment variables and sensible defaults.

### Creating your Local `.env` File

Copy `.env.example` to `.env`:

```powershell
# Windows PowerShell
Copy-Item .env.example .env

# Command Prompt (cmd)
copy .env.example .env
```

### Environment Variables Reference

| Variable | Description | Default | Example |
| :--- | :--- | :--- | :--- |
| `SERVER_HOST` | Interface IP for signaling server to bind | `0.0.0.0` | `0.0.0.0` |
| `SERVER_PORT` | Port for signaling server | `5000` | `5000` |
| `SIGNALING_SERVER_IP` | IPv4 address of the signaling server | `127.0.0.1` | `192.168.1.50` |
| `CLIENT_LISTEN_HOST` | Interface IP for client P2P listener | `0.0.0.0` | `0.0.0.0` |
| `CLIENT_LISTEN_PORT` | Port for direct incoming peer connections | `6001` | `6001` |
| `CLIENT_USERNAME` | (Optional) Preset client username | `""` | `anuva` |
| `LOG_LEVEL` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`) | `INFO` | `INFO` |
| `DEBUG_MODE` | Enable verbose handshake logging (`true`/`false`) | `false` | `false` |

> [!IMPORTANT]
> `.env` files contain machine-specific LAN IP addresses and local configurations. The `.gitignore` file is pre-configured to strictly exclude `.env` from being committed to Git. Only `.env.example` is tracked.

### Setting Variables via Terminal (Without a `.env` file)

You can also set environment variables directly in your shell session:

**Windows PowerShell**:
```powershell
$env:SIGNALING_SERVER_IP="192.168.1.50"
$env:CLIENT_LISTEN_PORT="6001"
```

**Windows Command Prompt (cmd)**:
```cmd
set SIGNALING_SERVER_IP=192.168.1.50
set CLIENT_LISTEN_PORT=6001
```

---

## 5. Same-Wi-Fi Networking & Multi-Device Setup

To chat between two separate physical devices on the same Wi-Fi router:

### Step 1: Find the Signaling Server's LAN IP Address (Device 1)

On **Device 1** (which will run the signaling server):
1. Open PowerShell and run:
   ```powershell
   ipconfig
   ```
2. Look for the **Wireless LAN adapter Wi-Fi** section and locate the **IPv4 Address** (e.g., `192.168.1.50` or `192.168.29.108`).
3. Alternatively, when you run `python server.py`, the server automatically detects and displays your active Wi-Fi LAN IP in the terminal banner!

### Step 2: Configure Windows Defender Firewall

By default, Windows Firewall blocks incoming TCP connections from other devices on the LAN.

On **Device 1** (Server host) and **Device 2** (Peer):
1. Ensure your Wi-Fi network profile is set to **Private** (Settings > Network & internet > Wi-Fi > Network profile type > Private).
2. Allow Python through Windows Firewall when prompted by Windows, OR open the required ports:
   ```powershell
   # Run in PowerShell as Administrator to allow Signaling Server (Port 5000)
   New-NetFirewallRule -DisplayName "P2P Chat Signaling Server" -Direction Inbound -LocalPort 5000 -Protocol TCP -Action Allow

   # Allow Client P2P Listener (Ports 6001-6010)
   New-NetFirewallRule -DisplayName "P2P Chat Client Direct" -Direction Inbound -LocalPort 6001-6010 -Protocol TCP -Action Allow
   ```

> [!WARNING]
> **Router Client Isolation / Guest Wi-Fi**: Many public Wi-Fi networks (hotels, cafes, guest networks) enable **AP Isolation** (Client Isolation), which blocks devices connected to the same Wi-Fi from talking to each other. Use a standard home/office Wi-Fi network or a mobile Wi-Fi hotspot where client isolation is disabled.

---

## 6. How to Run the Application

### Scenario A: Running Between Two Separate Devices on the Same Wi-Fi

#### 1. Start the Signaling Server on Device 1
```powershell
python server.py
```
*Output:*
```text
============================================================
      P2P CHAT SIGNALING SERVER (DISCOVERY ONLY)
============================================================
[*] Bound on:         0.0.0.0:5000
[*] Local LAN IP:     192.168.1.50
[*] Port:             5000
[*] Other devices on the Wi-Fi should connect to: 192.168.1.50
[*] Note: Chat messages are NEVER relayed through this server.
============================================================
[*] Waiting for client connections... Press Ctrl+C to stop.
```

#### 2. Start Client 1 on Device 1
In a second terminal on Device 1:
```powershell
# You can pass arguments directly: python client.py <username> <server_ip> <listen_port>
python client.py anuva 192.168.1.50 6001
```

#### 3. Start Client 2 on Device 2
On Device 2:
```powershell
python client.py rahul 192.168.1.50 6002
```

---

### Scenario B: Local Testing on a Single Machine

You can easily test the entire workflow on a single computer by opening 3 separate terminal tabs:

- **Terminal 1 (Server)**:
  ```powershell
  python server.py
  ```
- **Terminal 2 (Client 1 - Alice)**:
  ```powershell
  python client.py alice 127.0.0.1 6001
  ```
- **Terminal 3 (Client 2 - Bob)**:
  ```powershell
  python client.py bob 127.0.0.1 6002
  ```

---

## 7. Interactive CLI Commands

Once inside `client.py`, you will see the interactive prompt:

```text
========================================
       PYTHON P2P CLI CHAT
========================================
CLI > /help
```

| Command | Syntax | Description |
| :--- | :--- | :--- |
| `/users` | `/users` | Queries the signaling server and lists all online registered users. |
| `/chat` | `/chat <username>` | Looks up `<username>`'s IP & port, connects directly via TCP, and sets as active chat partner. |
| `/msg` | `/msg <username> <text>` | Sends a direct P2P message to `<username>`. (Connects automatically if not already connected). |
| Direct Chat | `<text>` | When an active chat partner is set (`CLI [username] >`), any text entered without a slash command is sent directly to them. |
| `/status` | `/status` | Displays signaling connection status, local LAN IP, listening port, and list of active direct P2P sessions. |
| `/close` | `/close [username]` | Gracefully closes the direct P2P TCP connection with a peer. |
| `/help` | `/help` | Displays command list and instructions. |
| `/exit` | `/exit` | Disconnects all active peer sessions and the signaling connection, then exits. |

---

## 8. Reliability, Error Handling, and Security

### Networking Reliability Features
- **TCP Length-Prefixed Framing**: Prevents packet fragmentation and concatenated message boundary corruption.
- **Port Conflict Recovery**: If a client's configured listening port is occupied (e.g. `WinError 10048`), the client automatically increments the port and binds cleanly.
- **Thread-Safe Console Output**: Incoming messages are synchronized using mutex locks to prevent garbling what the user is currently typing.
- **Peer Disconnection Detection**: Detects abrupt socket resets and EOF events cleanly without hanging or crashing threads.
- **Signaling Resilience**: If the signaling server goes offline, existing direct P2P chat sessions between clients remain active and unaffected.

### Security Limitations & Future Roadmap
- **Plaintext TCP**: Network packets travel in plaintext JSON across the local network. Anyone performing packet sniffing (e.g., Wireshark) on an unencrypted Wi-Fi network could inspect traffic.
- **No In-Transit Encryption**: As specified in the requirements, messages are not claimed to be encrypted.
- **Recommended Future Enhancement**: Wrap sockets in standard-library TLS (`ssl.create_default_context()` with self-signed or pre-shared certificates) for authenticated, end-to-end encrypted direct peer communication.

---

## 9. Automated Testing Suite

The repository includes a comprehensive automated test suite in `test_chat.py` covering 14 test scenarios:

```powershell
python test_chat.py -v
```

### Verified Test Cases:
1. `test_validate_ip`: Strict IPv4 address validation and prevention of invalid octets.
2. `test_validate_port`: Port range boundary checks (1024–65535).
3. `test_parse_bool`: Boolean string parsing (`true`, `false`, `1`, `0`).
4. `test_load_dotenv`: Custom zero-dependency `.env` file loader and precedence handling.
5. `test_lan_ip_detection`: Verification that local LAN IP discovery returns valid IPv4.
6. `test_send_and_recv_msg_roundtrip`: Length-prefixed framing and decoding across socket pairs.
7. `test_oversized_payload_rejection`: Payload size limit enforcement (`MAX_MESSAGE_SIZE`).
8. `test_registration_and_duplicate_rejection`: Signaling user registration and duplicate name prevention.
9. `test_online_user_discovery`: Registry queries and user listing exclusions.
10. `test_connect_request`: Endpoint resolution for valid and non-existent peers.
11. `test_strict_server_rejection_of_chat_messages`: **Critical security test** verifying the signaling server rejects and never relays chat messages.
12. `test_direct_p2p_chat_session`: End-to-end direct P2P TCP connection and bidirectional messaging.
13. `test_env_example_exists`: Verification of `.env.example` presence and keys.
14. `test_gitignore_rules`: Verification of `.gitignore` exclusion rules.

---

## 10. Publishing to GitHub

Follow these steps to publish this project to your GitHub account:

### Step 1: Create a New GitHub Repository
1. Log into your account at [GitHub](https://github.com/).
2. Click **New** repository.
3. Name your repository (e.g., `cli-chat` or `python-p2p-chat`).
4. Choose **Public** or **Private**.
5. Do **NOT** initialize with a README, .gitignore, or license (we already have them created locally).
6. Click **Create repository**.
7. Copy your repository's HTTPS or SSH URL (e.g., `https://github.com/<your-username>/cli-chat.git`).

### Step 2: Review Staged Files and Ensure No Secrets Are Committed
Run the following commands in PowerShell:

```powershell
# Check git status - note that .env is ignored and only .env.example is tracked
git status

# Stage all files
git add .

# Check git status again to verify staged files
git status
```

Verify that `.env` is **NOT** listed in the staged changes.

### Step 3: Commit and Push

```powershell
# Commit the project
git commit -m "Build Python P2P CLI chat application"

# Rename default branch to main
git branch -M main

# Add your GitHub remote (replace with your ACTUAL GitHub repository URL)
git remote add origin https://github.com/<your-username>/<your-repo-name>.git

# Push to GitHub
git push -u origin main
```
