"""
Peers Checker Script

Author: HaOtiC (GitHub: @HaOtiC)

This script checks the block heights of peers listed in a file or fetched from an RPC endpoint.
It compares the block heights of peers against the expected block height from a specified RPC endpoint.

Script inspired by https://github.com/bluefishismesu/bluefishismevd_initia/

Parameters:
1. rpc_url (str): The RPC URL to fetch the expected block height.
2. get_peers_from_file (str): Path to the file containing peers. If not present, peers are fetched from the RPC URL.
3. top_n (int): Number of top peers to save based on block height.
4. output_filename (str): The filename to save the top peers.
5. json_format (bool, optional, default=False): Save peers information in JSON format (include moniker, node_id, ip, port, full_peer, latency).
6. accepted_height_difference (int): Accepted difference between the expected height and the actual peer height.
7. max_latency (int, optional): Maximum latency to filter peers.

Example usage:
    python3 peers_checker.py https://rpc-initia.01node.com "" 30 top_peers.txt True 100 50
    python3 peers_checker.py https://rpc-initia.01node.com peers.txt 30 top_peers.txt True 100 50

Additional behavior:
- The script creates another file with the name "output_filename"_ids_only.txt that contains only peers' IDs in the format id,id,id,id...
"""

import socket
import time
import logging
import requests
import sys
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def check_connection(ip, port, timeout=2):
    start_time = time.time()
    try:
        with socket.create_connection((ip, port), timeout):
            end_time = time.time()
            return True, (end_time - start_time) * 1000  # Convert to milliseconds
    except (socket.timeout, socket.error):
        return False, None


def get_latest_block_height(ip, rpc_port):
    url_http = f"http://{ip}:{rpc_port}/status"
    try:
        response = requests.get(url_http, timeout=1)
        if response.status_code == 200:
            result = response.json()
            latest_block_height = int(result["result"]["sync_info"]["latest_block_height"])
            moniker = result["result"]["node_info"]["moniker"]
            node_id = result["result"]["node_info"]["id"]
            return latest_block_height, moniker, node_id
    except requests.RequestException:
        pass
    return None, "", ""


def fetch_expected_height(rpc_url):
    try:
        response = requests.get(f"{rpc_url}/status", timeout=2)
        if response.status_code == 200:
            result = response.json()
            expected_height = int(result["result"]["sync_info"]["latest_block_height"])
            logging.info(f"Expected block height fetched from {rpc_url}: {expected_height}")
            return expected_height
    except requests.RequestException as e:
        logging.error(f"Failed to fetch expected height from {rpc_url}: {e}")
    return None


def fetch_peers(rpc_url):
    try:
        response = requests.get(f"{rpc_url}/net_info", timeout=2)
        if response.status_code == 200:
            result = response.json()
            peers = result["result"]["peers"]
            return [f"{peer['node_info']['id']}@{peer['remote_ip']}:{peer['node_info']['listen_addr'].split(':')[-1]}"
                    for peer in peers]
    except requests.RequestException as e:
        logging.error(f"Failed to fetch peers from {rpc_url}: {e}")
    return []


def parse_file(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = file.read().strip().split(',')
        return lines
    except IOError as e:
        logging.error(f"Failed to read the file from {file_path}: {e}")
        return []


def check_nodes(lines, expected_height, max_latency, accepted_height_difference):
    successful_connections = []
    moniker_info = []
    total_lines = len(lines)

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ip_port = {
            executor.submit(process_line, line, expected_height, max_latency, accepted_height_difference): line for line in lines
        }

        processed_count = 0
        for future in as_completed(future_to_ip_port):
            line = future_to_ip_port[future]
            try:
                result = future.result()
                if result:
                    successful_connections.append((result[0], result[1], result[2]))
                    moniker_info.append(result[2])
                processed_count += 1
                logging.info(f"Processed {processed_count}/{total_lines} entries.")
            except Exception as e:
                logging.error(f"Error processing line {line}: {e}")
    return successful_connections, moniker_info


def process_line(line, expected_height, max_latency, accepted_height_difference):
    parts = line.split('@')
    if len(parts) == 2:
        ip_port = parts[1].split(':')
        if len(ip_port) == 2:
            ip = ip_port[0]
            port = int(ip_port[1])
            success, latency = check_connection(ip, port)
            if success and (max_latency is None or latency <= max_latency):
                block_height, moniker, node_id = get_latest_block_height(ip, port + 1)
                if block_height is not None:
                    if abs(block_height - expected_height) <= accepted_height_difference:
                        logging.info(
                            f"block_height {moniker} {ip}:{port} with {block_height} height and {int(latency)} ms latency")
                        moniker_info = {
                            "moniker": moniker,
                            "node_id": node_id,
                            "ip": ip,
                            "port": port,
                            "full_peer": f"{node_id}@{ip}:{port}",
                            "latency": int(latency)
                        }
                        return line, block_height, moniker_info
    return None


def save_top_connections(connections, output_filename, top_n=40):
    connections.sort(key=lambda x: x[1], reverse=True)
    top_connections = connections[:top_n]
    with open(output_filename, 'w') as file:
        first_entry = True
        for conn in top_connections:
            logging.info(f"Connection: {conn[0]}, Block Height: {conn[1]}, Latency: {conn[2]['latency']} ms")
            if first_entry:
                file.write(conn[0])
                first_entry = False
            else:
                file.write(',' + conn[0])
    logging.info(f"Saved top {top_n} connections to {output_filename}.")
    save_ids_only(top_connections, output_filename)
    return len(top_connections)


def save_ids_only(connections, output_filename):
    ids_only_filename = output_filename.replace('.txt', '_ids_only.txt')
    with open(ids_only_filename, 'w') as file:
        first_entry = True
        for conn in connections:
            if first_entry:
                file.write(conn[2]['node_id'])
                first_entry = False
            else:
                file.write(',' + conn[2]['node_id'])
    logging.info(f"Saved peer IDs to {ids_only_filename}.")


def save_moniker_info(moniker_info, output_filename):
    with open(output_filename, 'w') as file:
        json.dump(moniker_info, file, indent=4)
    logging.info(f"Saved moniker information to {output_filename}.")


# Main script execution
if __name__ == "__main__":
    try:
        start_time = time.time()
        rpc_url = sys.argv[1]
        get_peers_from_file = sys.argv[2]
        top_n = int(sys.argv[3])
        output_filename = sys.argv[4]
        json_format = sys.argv[5].lower() == 'true'
        accepted_height_difference = int(sys.argv[6])
        max_latency = int(sys.argv[7]) if len(sys.argv) > 7 else None

        expected_height = fetch_expected_height(rpc_url)
        if expected_height is None:
            logging.error("Failed to fetch expected block height. Exiting.")
            sys.exit(1)

        logging.info(f"Expected block height is {expected_height}")

        if get_peers_from_file:
            lines = parse_file(get_peers_from_file)
            if not lines:
                logging.error(f"Failed to fetch peers from file {get_peers_from_file}. Exiting.")
                sys.exit(1)
            peers_source = get_peers_from_file
        else:
            lines = fetch_peers(rpc_url)
            if not lines:
                logging.error(f"Failed to fetch peers from RPC {rpc_url}. Exiting.")
                sys.exit(1)
            peers_source = f"{rpc_url}/net_info"

        connections, moniker_info = check_nodes(lines, expected_height, max_latency, accepted_height_difference)

        matched_nodes = len(connections)
        saved_nodes = save_top_connections(connections, output_filename, top_n)

        if json_format:
            moniker_output_filename = output_filename.replace('.txt', '_moniker_objs.json')
            save_moniker_info(moniker_info, moniker_output_filename)

        end_time = time.time()
        total_time = end_time - start_time

        logging.info(f"Script run time: {total_time:.2f} seconds")
        logging.info(f"Total nodes matched: {matched_nodes}")
        logging.info(f"Total nodes saved: {saved_nodes}")
        logging.info(f"Peers were fetched from: {peers_source}")
        logging.info("Processing completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
