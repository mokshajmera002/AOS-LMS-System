import grpc
import raft_pb2
import raft_pb2_grpc
import threading

class RaftClientServer:
    def __init__(self, servers):
        self.servers = servers  # List of server addresses
        self.leader_id = None
        self.lock = threading.Lock()

    def find_leader(self):
        for server_address in self.servers:
            try:
                with grpc.insecure_channel(server_address) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    # Send a ClientRequest with empty command to probe for leader
                    request = raft_pb2.ClientRequestMessage(command="")
                    response = stub.ClientRequest(request, timeout=1)
                    if response.success:
                        # This server is the leader
                        self.leader_id = server_address
                        print(f"Leader found: {self.leader_id}")
                        return True
                    else:
                        if response.leaderId:
                            self.leader_id = response.leaderId
                            print(f"Redirecting to leader at: {self.leader_id}")
                            return True
                        else:
                            continue
            except Exception as e:
                continue
        return False

    def send_command(self, command):
        with self.lock:
            if self.leader_id is None:
                found = self.find_leader()
                if not found:
                    print("Leader not found in the cluster.")
                    return False

            try:
                with grpc.insecure_channel(self.leader_id) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    request = raft_pb2.ClientRequestMessage(command=command)
                    response = stub.ClientRequest(request, timeout=1)
                    if response.success:
                        print(f"Command '{command}' successfully replicated to the cluster.")
                        return True
                    else:
                        if response.leaderId:
                            self.leader_id = response.leaderId
                            print(f"Leader changed to: {self.leader_id}. Retrying command.")
                            return self.send_command(command)
                        else:
                            self.leader_id = None
                            print("Leader information not provided. Retrying leader discovery.")
                            return self.send_command(command)
            except Exception as e:
                print(f"Failed to send command to leader {self.leader_id}: {e}")
                self.leader_id = None
                return self.send_command(command)

    def handle_client_request(self, command):
        result = self.send_command(command)
        return result

def run_raft_client():
    servers = [f"127.0.0.1:5000{i}" for i in range(3)]
    raft_client_server = RaftClientServer(servers)

    while True:
        try:
            command = input("Enter command (e.g., 'SET key value' or 'GET key'): ")
            if command.lower() == 'exit':
                print("Exiting Raft Client.")
                break
            if not command.strip():
                continue
            result = raft_client_server.handle_client_request(command)
            if result:
                print("Command executed successfully.")
            else:
                print("Failed to execute command.")
        except KeyboardInterrupt:
            print("\nExiting Raft Client.")
            break

if __name__ == "__main__":
    run_raft_client()
