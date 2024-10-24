import sys
from concurrent import futures
import time
import fire
import grpc
import raft_pb2
import raft_pb2_grpc
import random
import threading
import signal
import pickle
import os

class RaftServer():
    def __init__(self, sid, self_address, peer_addresses):
        self.sid = int(sid)
        self.socket_address = self_address
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServicer_to_server(RaftService(self), self.server)
        self.server.add_insecure_port(self.socket_address)

        # State variables
        self.currentTerm = 0
        self.votedFor = None
        self.state = 'follower'
        self.leaderId = None
        self.peers = [addr for addr in peer_addresses if addr != self.socket_address]

        # Log and State Machine
        self.log = []  # List of log entries
        self.commitIndex = 0
        self.lastApplied = 0
        self.state_machine = {}  # Simple key-value store

        # Persistent state
        self.state_file = f"raft_state_{self.sid}.pkl"
        self.load_state()

        # Initialize nextIndex and matchIndex
        self.nextIndex = {}  # nextIndex for each follower
        self.matchIndex = {}  # matchIndex for each follower

        # Election timer reset event
        self.election_timer_timeout = random.uniform(0.5, 0.8)
        self.election_reset_event = threading.Event()
        self.lock = threading.RLock()  # Use reentrant lock to prevent deadlock

    def isLeader(self):
        return self.state == 'leader'

    def start(self):
        self.server.start()
        print(f"Server {self.sid} started at {self.socket_address}")

    def wait_for_termination(self):
        self.server.wait_for_termination()

    def request_heart_beats(self):
        threading.Thread(target=self.election_timer, daemon=True).start()

        while True:
            if self.state == 'leader':
                self.send_append_entries()
                time.sleep(0.2)
            else:
                time.sleep(0.2)

    def election_timer(self):
        while True:
            timeout = self.election_timer_timeout
            event_is_set = self.election_reset_event.wait(timeout)
            if event_is_set:
                self.election_reset_event.clear()
                continue
            else:
                if self.state != 'leader':
                    print(f"Server {self.sid} election timeout. Starting election.")
                    self.start_election()

    def send_append_entries(self):
        for peer in self.peers:
            threading.Thread(target=self.send_append_entries_to_peer, args=(peer,), daemon=True).start()

    def send_append_entries_to_peer(self, peer):
        with grpc.insecure_channel(peer) as channel:
            stub = raft_pb2_grpc.RaftStub(channel)
            while True:
                with self.lock:
                    if peer not in self.nextIndex:
                        # Initialize nextIndex for new follower
                        self.nextIndex[peer] = self.get_last_log_index() + 1
                        self.matchIndex[peer] = 0

                    next_index = self.nextIndex[peer]
                    prevLogIndex = next_index - 1
                    prevLogTerm = self.log[prevLogIndex - 1]['term'] if prevLogIndex > 0 else 0
                    entries = []
                    if len(self.log) >= next_index:
                        entries = [raft_pb2.LogEntry(term=entry['term'], command=entry['command']) 
                                  for entry in self.log[next_index - 1:]]
                    else:
                        entries = []  # Heartbeat with no entries

                    request = raft_pb2.AppendEntriesRequest(
                        term=self.currentTerm,
                        leaderId=self.sid,
                        prevLogIndex=prevLogIndex,
                        prevLogTerm=prevLogTerm,
                        entries=entries,
                        leaderCommit=self.commitIndex,
                    )
                try:
                    response = stub.AppendEntries(request, timeout=1)
                    if response.term > self.currentTerm:
                        with self.lock:
                            self.currentTerm = response.term
                            self.state = 'follower'
                            self.votedFor = None
                            self.save_state()
                        break
                    if response.success:
                        with self.lock:
                            self.matchIndex[peer] = self.nextIndex[peer] + len(entries) - 1
                            self.nextIndex[peer] = self.matchIndex[peer] + 1
                            self.advance_commit_index()
                        break  # Successful append
                    else:
                        with self.lock:
                            self.nextIndex[peer] = max(1, self.nextIndex[peer] - 1)
                except Exception as e:
                    print(f"Leader {self.sid} failed to send AppendEntries to {peer}: {e}")
                    break  # Exit the loop and retry later

    def start_election(self):
        with self.lock:
            self.state = 'candidate'
            self.currentTerm += 1
            self.votedFor = self.sid
            self.save_state()
            votes_received = 1  # Vote for self

        vote_lock = threading.Lock()
        votes = [votes_received]

        def request_vote(peer):
            nonlocal votes
            with grpc.insecure_channel(peer) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                with self.lock:
                    request = raft_pb2.RequestVoteRequest(
                        term=self.currentTerm,
                        candidateId=self.sid,
                        lastLogIndex=self.get_last_log_index(),
                        lastLogTerm=self.get_last_log_term(),
                    )
                try:
                    #This also helps in getting elected faster
                    response = stub.RequestVote(request, timeout=random.uniform(0.8, 2.5))
                    with self.lock:
                        if response.voteGranted:
                            with vote_lock:
                                votes[0] += 1
                                if votes[0] > (len(self.peers) + 1) // 2 and self.state == 'candidate':
                                    print(f"Candidate {self.sid} won the election with {votes[0]} votes.")
                                    self.state = 'leader'
                                    self.leaderId = self.sid
                                    # Initialize nextIndex and matchIndex
                                    last_index = self.get_last_log_index()
                                    self.nextIndex = {p: last_index + 1 for p in self.peers}
                                    self.matchIndex = {p: 0 for p in self.peers}
                                    self.send_append_entries()
                        elif response.term > self.currentTerm:
                            self.currentTerm = response.term
                            self.state = 'follower'
                            self.votedFor = None
                            self.save_state()
                except Exception as e:
                    print(f"Candidate {self.sid} failed to request vote from {peer}: {e}")

        threads = []
        for peer in self.peers:
            thread = threading.Thread(target=request_vote, args=(peer,), daemon=True)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    def get_last_log_index(self):
        return len(self.log)

    def get_last_log_term(self):
        if self.log:
            return self.log[-1]['term']
        else:
            return 0

    def advance_commit_index(self):
        for N in range(self.commitIndex + 1, self.get_last_log_index() + 1):
            count = 1  # Include leader
            for peer in self.peers:
                if self.matchIndex.get(peer, 0) >= N:
                    count += 1
            if count > (len(self.peers) + 1) // 2 and self.log[N - 1]['term'] == self.currentTerm:
                self.commitIndex = N
                self.apply_entries()
            else:
                break

    def apply_entries(self):
        while self.lastApplied < self.commitIndex:
            self.lastApplied += 1
            entry = self.log[self.lastApplied - 1]
            # Apply the entry's command to the state machine
            command = entry['command']
            # For simplicity, assume commands are in the format "SET key value" or "GET key"
            parts = command.strip().split()
            if len(parts) == 3 and parts[0].upper() == 'SET':
                key, value = parts[1], parts[2]
                self.state_machine[key] = value
                print(f"Server {self.sid} applied command: {command}")
            elif len(parts) == 2 and parts[0].upper() == 'GET':
                key = parts[1]
                value = self.state_machine.get(key, None)
                print(f"Server {self.sid} GET command: {key} => {value}")
            else:
                print(f"Server {self.sid} received unknown command: {command}")
        self.save_state()

    def save_state(self):
        with self.lock:
            state = {
                'currentTerm': self.currentTerm,
                'votedFor': self.votedFor,
                'log': self.log,
                'commitIndex': self.commitIndex,
                'lastApplied': self.lastApplied,
                'state_machine': self.state_machine,
            }
            with open(self.state_file, 'wb') as f:
                pickle.dump(state, f)
            # Optional: print(f"Server {self.sid} state saved.")

    def load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'rb') as f:
                state = pickle.load(f)
                self.currentTerm = state['currentTerm']
                self.votedFor = state['votedFor']
                self.log = state['log']
                self.commitIndex = state['commitIndex']
                self.lastApplied = state['lastApplied']
                self.state_machine = state['state_machine']
            print(f"Server {self.sid} state loaded.")
        else:
            self.log = []
            self.commitIndex = 0
            self.lastApplied = 0
            self.state_machine = {}

    def shutdown(self, signum, frame):
        print(f"Server {self.sid} is shutting down.")
        self.save_state()
        self.server.stop(0)  # Stops the server immediately
        sys.exit(0)

class RaftService(raft_pb2_grpc.RaftServicer):
    def __init__(self, server: RaftServer):
        self.server = server

    def AppendEntries(self, request, context):
        with self.server.lock:
            print(f"Server {self.server.sid} received AppendEntries from Leader {request.leaderId} with term {request.term}")
            response = raft_pb2.AppendEntriesResponse()
            if request.term < self.server.currentTerm:
                response.term = self.server.currentTerm
                response.success = False
                return response

            if request.term > self.server.currentTerm:
                self.server.currentTerm = request.term
                self.server.votedFor = None
                self.server.save_state()

            self.server.state = 'follower'
            self.server.leaderId = request.leaderId
            self.server.election_reset_event.set()

            # Log consistency check
            if request.prevLogIndex > 0:
                if len(self.server.log) < request.prevLogIndex:
                    response.term = self.server.currentTerm
                    response.success = False
                    return response
                elif self.server.log[request.prevLogIndex - 1]['term'] != request.prevLogTerm:
                    # Conflict detected
                    self.server.log = self.server.log[:request.prevLogIndex - 1]
                    self.server.save_state()
                    response.term = self.server.currentTerm
                    response.success = False
                    return response

            # Append new entries
            for i, entry in enumerate(request.entries):
                index = request.prevLogIndex + i + 1
                if len(self.server.log) >= index:
                    if self.server.log[index - 1]['term'] != entry.term:
                        # Conflict detected, delete the entry and all that follow
                        self.server.log = self.server.log[:index - 1]
                        self.server.log.append({'term': entry.term, 'command': entry.command})
                else:
                    self.server.log.append({'term': entry.term, 'command': entry.command})
            self.server.save_state()

            # Update commit index
            if request.leaderCommit > self.server.commitIndex:
                self.server.commitIndex = min(request.leaderCommit, len(self.server.log))
                # Apply committed entries to state machine
                self.server.apply_entries()

            response.term = self.server.currentTerm
            response.success = True
            return response

    def RequestVote(self, request, context):
        with self.server.lock:
            print(f"Server {self.server.sid} received RequestVote from Candidate {request.candidateId} with term {request.term}")
            response = raft_pb2.RequestVoteResponse()
            response.term = self.server.currentTerm

            if request.term < self.server.currentTerm:
                response.voteGranted = False
            else:
                if (self.server.votedFor is None or self.server.votedFor == request.candidateId) and \
                   (request.lastLogTerm > self.server.get_last_log_term() or
                   (request.lastLogTerm == self.server.get_last_log_term() and
                    request.lastLogIndex >= self.server.get_last_log_index())):
                    self.server.votedFor = request.candidateId
                    self.server.currentTerm = request.term
                    self.server.save_state()
                    response.voteGranted = True
                else:
                    response.voteGranted = False

            return response

    def ClientRequest(self, request, context):
        with self.server.lock:
            if self.server.state != 'leader':
                # Not the leader, return redirect
                response = raft_pb2.ClientResponseMessage(
                    success=False,
                    message="Not the leader",
                    leaderId=self.server.leaderId if self.server.leaderId is not None else ""
                )
                print(f"Server {self.server.sid} is not the leader. Redirecting to {self.server.leaderId}")
                return response
            else:
                # Leader: Append the command to the log and start replication
                print(f"Leader {self.server.sid} received ClientRequest with command: {request.command}")
                entry = {'term': self.server.currentTerm, 'command': request.command}
                self.server.log.append(entry)
                self.server.save_state()
                # Start replication (send AppendEntries to followers)
                self.server.send_append_entries()
                response = raft_pb2.ClientResponseMessage(
                    success=True,
                    message="Command accepted",
                    leaderId=""
                )
                return response

def run_server(sid: str, self_address: str, peer_addresses: str):
    peer_list = peer_addresses.split(',')
    server = RaftServer(sid, self_address, peer_list)
    server.start()

    # Register signal handlers
    signal.signal(signal.SIGINT, server.shutdown)
    signal.signal(signal.SIGTERM, server.shutdown)

    server.request_heart_beats()
    server.wait_for_termination()

if __name__ == "__main__":
    try:
        fire.Fire(run_server)
    except Exception as e:
        print(e)
