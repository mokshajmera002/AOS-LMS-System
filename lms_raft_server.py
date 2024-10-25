import sys 
from concurrent import futures
import time
import fire
import grpc
import threading
import signal
import pickle
import os
import base64
import random
import datetime
import sqlite3
import jwt
from jwt import InvalidTokenError

import raft_pb2
import raft_pb2_grpc
import lms_pb2
import lms_pb2_grpc
import llm_server_pb2
import llm_server_pb2_grpc

# Constants for JWT
JWT_SECRET = 'software_project_management'
JWT_ALGORITHM = 'HS256'

class LMSService(lms_pb2_grpc.LMSServicer):
    def __init__(self, sid, llm_address=None):
        # Initialize LMS database and other necessary components
        self.sid = sid
        self.conn = sqlite3.connect(f'LMS_DATABASE_{self.sid}.db', check_same_thread=False)
        self.create_tables()
        self.create_default_admin()
        self.llm_server = llm_address
    
    def create_tables(self):
        cursor = self.conn.cursor()
        # Users table
        cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            username TEXT UNIQUE,
                            password TEXT,
                            role INTEGER
                        )''')
        # Posts table
        cursor.execute('''CREATE TABLE IF NOT EXISTS posts (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            title TEXT,
                            description TEXT,
                            type INTEGER,
                            filename TEXT,
                            content BLOB,
                            timestamp TEXT
                        )''')
        # Solutions table
        cursor.execute('''CREATE TABLE IF NOT EXISTS solutions (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            post_id INTEGER,
                            student_id INTEGER,
                            filename TEXT,
                            content BLOB,
                            timestamp TEXT,
                            grade REAL,
                            feedback TEXT
                        )''')
        # Queries table
        cursor.execute('''CREATE TABLE IF NOT EXISTS queries (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            student_id INTEGER,
                            content TEXT,
                            timestamp TEXT,
                            target INTEGER,
                            llm_response TEXT
                        )''')
        # Replies table
        cursor.execute('''CREATE TABLE IF NOT EXISTS replies (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            query_id INTEGER,
                            user_id INTEGER,
                            content TEXT,
                            timestamp TEXT
                        )''')
        self.conn.commit()

    def create_default_admin(self):
        cursor = self.conn.cursor()
        # Check if admin user exists
        cursor.execute('SELECT * FROM users WHERE username=?', ('admin',))
        if not cursor.fetchone():
            # Create default admin user
            cursor.execute('INSERT INTO users (username, password, role) VALUES (?, ?, ?)',
                           ('admin', 'admin123', lms_pb2.ADMIN))
            self.conn.commit()

    def authenticate(self, context):
        if context is None:
            # Assume system admin privileges when context is None (called from apply_entries)
            return {'user_id': 0, 'username': 'system', 'role': lms_pb2.ADMIN}
        metadata = dict(context.invocation_metadata())
        token = metadata.get('authorization')
        if not token:
            context.set_details('Unauthorized')
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            return None
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            return payload
        except InvalidTokenError:
            context.set_details('Invalid token')
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            return None

    def Login(self, request, context):
        cursor = self.conn.cursor()
        cursor.execute('SELECT id, username, password, role FROM users WHERE username=?', (request.username,))
        row = cursor.fetchone()
        if row and row[2] == request.password:
            # Create JWT token
            payload = {
                'user_id': row[0],
                'username': row[1],
                'role': row[3],
                'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
            }
            token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
            return lms_pb2.LoginResponse(token=token)
        else:
            context.set_details('Invalid username or password')
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            return lms_pb2.LoginResponse()

    def CreateUser(self, request, context):
        # Check if the request is from an authenticated admin
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.UserResponse()
        if context is not None and payload['role'] != lms_pb2.ADMIN:
            context.set_details('Permission denied')
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return lms_pb2.UserResponse()
        cursor = self.conn.cursor()
        try:
            cursor.execute('INSERT INTO users (username, password, role) VALUES (?, ?, ?)',
                           (request.username, request.password, request.role))
            self.conn.commit()
            user_id = cursor.lastrowid
            user = lms_pb2.User(
                id=user_id,
                username=request.username,
                role=request.role
            )
            return lms_pb2.UserResponse(user=user)
        except sqlite3.IntegrityError:
            context.set_details('Username already exists')
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            return lms_pb2.UserResponse()

    def ListUsers(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.UserList()
        if context is not None and payload['role'] != lms_pb2.ADMIN:
            context.set_details('Permission denied')
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return lms_pb2.UserList()
        cursor = self.conn.cursor()
        cursor.execute('SELECT id, username, role FROM users')
        users = []
        for row in cursor.fetchall():
            user = lms_pb2.User(
                id=row[0],
                username=row[1],
                role=row[2]
            )
            users.append(user)
        return lms_pb2.UserList(users=users)

    def PostContent(self, request, context):
        # Authenticate user
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.PostResponse()
        
        if context is not None:
            # Perform role check
            if payload['role'] != lms_pb2.INSTRUCTOR:
                context.set_details('Permission denied')
                context.set_code(grpc.StatusCode.PERMISSION_DENIED)
                return lms_pb2.PostResponse()
        
        # Proceed with operation
        cursor = self.conn.cursor()
        timestamp = datetime.datetime.utcnow().isoformat()
        # Save the file content directly in the database
        filename = request.filename
        content = request.content  # BLOB data
        cursor.execute('INSERT INTO posts (title, description, type, filename, content, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                       (request.title, request.description, request.type, filename, content, timestamp))
        self.conn.commit()
        post_id = cursor.lastrowid
        post = lms_pb2.Post(
            id=post_id,
            title=request.title,
            description=request.description,
            type=request.type,
            filename=filename,
            timestamp=timestamp
        )
        return lms_pb2.PostResponse(post=post)

    def GetPosts(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.PostList()
        cursor = self.conn.cursor()
        cursor.execute('SELECT id, title, description, type, filename, timestamp FROM posts')
        posts = []
        for row in cursor.fetchall():
            post = lms_pb2.Post(
                id=row[0],
                title=row[1],
                description=row[2],
                type=row[3],
                filename=row[4],
                timestamp=row[5]
            )
            posts.append(post)
        return lms_pb2.PostList(posts=posts)

    def DownloadPost(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.PostResponse()
        
        if context is not None:
            # Perform role check if necessary (e.g., any authenticated user can download)
            pass  # Assuming all authenticated users can download posts
        
        cursor = self.conn.cursor()
        cursor.execute('SELECT id, title, description, type, filename, content, timestamp FROM posts WHERE id=?', (request.id,))
        row = cursor.fetchone()
        if row:
            content = row[5] if row[5] else b''
            post = lms_pb2.Post(
                id=row[0],
                title=row[1],
                description=row[2],
                type=row[3],
                filename=row[4],
                content=content,
                timestamp=row[6]
            )
            return lms_pb2.PostResponse(post=post)
        else:
            if context is not None:
                context.set_details('Post not found')
                context.set_code(grpc.StatusCode.NOT_FOUND)
            return lms_pb2.PostResponse()

    def UploadSolution(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.SolutionResponse()
        
        if context is not None:
            # Perform role check
            if payload['role'] != lms_pb2.STUDENT:
                context.set_details('Permission denied')
                context.set_code(grpc.StatusCode.PERMISSION_DENIED)
                return lms_pb2.SolutionResponse()
        
        cursor = self.conn.cursor()
        timestamp = datetime.datetime.utcnow().isoformat()
        # Save the file content directly in the database
        filename = request.filename
        content = request.content  # BLOB data
        cursor.execute('''INSERT INTO solutions (post_id, student_id, filename, content, timestamp)
                          VALUES (?, ?, ?, ?, ?)''',
                       (request.post_id, payload['user_id'], filename, content, timestamp))
        self.conn.commit()
        solution_id = cursor.lastrowid
        solution = lms_pb2.Solution(
            id=solution_id,
            post_id=request.post_id,
            student_id=payload['user_id'],
            filename=filename,
            timestamp=timestamp,
            grade=0.0
        )
        return lms_pb2.SolutionResponse(solution=solution)

    def GetSolutions(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.SolutionList()
        if context is not None and payload['role'] != lms_pb2.INSTRUCTOR:
            context.set_details('Permission denied')
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return lms_pb2.SolutionList()
        cursor = self.conn.cursor()
        cursor.execute('''SELECT id, post_id, student_id, filename, content, timestamp, grade
                          FROM solutions WHERE post_id=?''', (request.id,))
        solutions = []
        for row in cursor.fetchall():
            solution = lms_pb2.Solution(
                id=row[0],
                post_id=row[1],
                student_id=row[2],
                filename=row[3],
                timestamp=row[5],
                grade=row[6] if row[6] is not None else 0.0
            )
            solutions.append(solution)
        return lms_pb2.SolutionList(solutions=solutions)

    def DownloadSolution(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.SolutionResponse()
        
        if context is not None:
            # Perform role check
            if payload['role'] != lms_pb2.INSTRUCTOR:
                context.set_details('Permission denied')
                context.set_code(grpc.StatusCode.PERMISSION_DENIED)
                return lms_pb2.SolutionResponse()
        
        cursor = self.conn.cursor()
        cursor.execute('SELECT id, post_id, student_id, filename, content, timestamp, grade FROM solutions WHERE id=?', (request.id,))
        row = cursor.fetchone()
        if row:
            content = row[4] if row[4] else b''
            solution = lms_pb2.Solution(
                id=row[0],
                post_id=row[1],
                student_id=row[2],
                filename=row[3],
                content=content,
                timestamp=row[5],
                grade=row[6] if row[6] is not None else 0.0
            )
            return lms_pb2.SolutionResponse(solution=solution)
        else:
            if context is not None:
                context.set_details('Solution not found')
                context.set_code(grpc.StatusCode.NOT_FOUND)
            return lms_pb2.SolutionResponse()

    def AssignGrade(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.SolutionResponse()
        if context is not None:
            # Perform role check
            if payload['role'] != lms_pb2.INSTRUCTOR:
                context.set_details('Permission denied')
                context.set_code(grpc.StatusCode.PERMISSION_DENIED)
                return lms_pb2.SolutionResponse()
        
        cursor = self.conn.cursor()
        # Update the grade for the specified solution
        cursor.execute('UPDATE solutions SET grade=? WHERE id=?', (request.grade, request.solution_id))
        self.conn.commit()
        # Retrieve the updated solution
        cursor.execute('SELECT id, post_id, student_id, filename, content, timestamp, grade FROM solutions WHERE id=?', (request.solution_id,))
        row = cursor.fetchone()
        if row:
            solution = lms_pb2.Solution(
                id=row[0],
                post_id=row[1],
                student_id=row[2],
                filename=row[3],
                timestamp=row[5],
                grade=row[6]
            )
            return lms_pb2.SolutionResponse(solution=solution)
        else:
            if context is not None:
                context.set_details('Solution not found')
                context.set_code(grpc.StatusCode.NOT_FOUND)
            return lms_pb2.SolutionResponse()

    def ViewGrades(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.SolutionList()
        if context is not None:
            # Perform role check
            if payload['role'] != lms_pb2.STUDENT:
                context.set_details('Permission denied')
                context.set_code(grpc.StatusCode.PERMISSION_DENIED)
                return lms_pb2.SolutionList()
        
        cursor = self.conn.cursor()
        cursor.execute('''SELECT id, post_id, student_id, filename, content, timestamp, grade, feedback
                          FROM solutions WHERE student_id=?''', (payload['user_id'],))
        solutions = []
        for row in cursor.fetchall():
            solution = lms_pb2.Solution(
                id=row[0],
                post_id=row[1],
                student_id=row[2],
                filename=row[3],
                timestamp=row[5],
                grade=row[6] if row[6] is not None else 0.0,
                feedback=row[7] if row[7] is not None else "Pending..."
            )
            solutions.append(solution)
        return lms_pb2.SolutionList(solutions=solutions)

    def PostQuery(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.Query()
        if context is not None:
            # Perform role check
            if payload['role'] != lms_pb2.STUDENT:
                context.set_details('Permission denied')
                context.set_code(grpc.StatusCode.PERMISSION_DENIED)
                return lms_pb2.Query()
        
        cursor = self.conn.cursor()
        timestamp = datetime.datetime.utcnow().isoformat()

        llm_response = ''
        if request.target == lms_pb2.LLM:
            # Insert with a placeholder response
            llm_response = 'LLM response pending...'
            cursor.execute(
                'INSERT INTO queries (student_id, content, timestamp, target, llm_response) VALUES (?, ?, ?, ?, ?)',
                (payload['user_id'], request.content, timestamp, request.target, llm_response)
            )
            self.conn.commit()
            query_id = cursor.lastrowid
            # Start background thread to generate and update LLM response
            threading.Thread(target=self.generate_and_update_llm_response, args=(query_id, request.content), daemon=True).start()
            # Return immediate confirmation to the client
            query = lms_pb2.Query(
                id=query_id,
                student_id=payload['user_id'],
                content=request.content,
                timestamp=timestamp,
                target=request.target,
                llm_response=llm_response
            )
            return query
        else:
            # Handle non-LLM queries as usual
            cursor.execute(
                'INSERT INTO queries (student_id, content, timestamp, target, llm_response) VALUES (?, ?, ?, ?, ?)',
                (payload['user_id'], request.content, timestamp, request.target, llm_response)
            )
            self.conn.commit()
            query_id = cursor.lastrowid
            query = lms_pb2.Query(
                id=query_id,
                student_id=payload['user_id'],
                content=request.content,
                timestamp=timestamp,
                target=request.target,
                llm_response=llm_response
            )
            return query

    def GetQueries(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.QueryList()
        cursor = self.conn.cursor()
        cursor.execute('SELECT id, student_id, content, timestamp, target, llm_response FROM queries')
        queries = []
        for row in cursor.fetchall():
            query = lms_pb2.Query(
                id=row[0],
                student_id=row[1],
                content=row[2],
                timestamp=row[3],
                target=row[4],
                llm_response=row[5] if row[5] else ''
            )
            queries.append(query)
        return lms_pb2.QueryList(queries=queries)

    def PostReply(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.Reply()
        if context is not None:
            # Perform role check
            if payload['role'] not in [lms_pb2.INSTRUCTOR, lms_pb2.ADMIN]:
                context.set_details('Permission denied')
                context.set_code(grpc.StatusCode.PERMISSION_DENIED)
                return lms_pb2.Reply()
        
        cursor = self.conn.cursor()
        timestamp = datetime.datetime.utcnow().isoformat()
        cursor.execute(
            'INSERT INTO replies (query_id, user_id, content, timestamp) VALUES (?, ?, ?, ?)',
            (request.query_id, payload['user_id'], request.content, timestamp)
        )
        self.conn.commit()
        reply_id = cursor.lastrowid
        reply = lms_pb2.Reply(
            id=reply_id,
            query_id=request.query_id,
            user_id=payload['user_id'],
            content=request.content,
            timestamp=timestamp
        )
        return reply

    def GetReplies(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.ReplyList()
        cursor = self.conn.cursor()
        cursor.execute(
            '''SELECT replies.id, replies.query_id, replies.user_id, replies.content, replies.timestamp, users.username 
               FROM replies 
               JOIN users ON replies.user_id = users.id 
               WHERE query_id=?''',
            (request.id,)
        )
        replies = []
        for row in cursor.fetchall():
            reply = lms_pb2.Reply(
                id=row[0],
                query_id=row[1],
                user_id=row[2],
                content=row[3],
                timestamp=row[4],
                username=row[5]
            )
            replies.append(reply)
        return lms_pb2.ReplyList(replies=replies)
    
    def AddFeedback(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.SolutionResponse()
        if context is not None:
            # Perform role check
            if payload['role'] != lms_pb2.INSTRUCTOR:
                context.set_details('Permission denied')
                context.set_code(grpc.StatusCode.PERMISSION_DENIED)
                return lms_pb2.SolutionResponse()
        
        cursor = self.conn.cursor()
        # Update the feedback for the specified solution
        cursor.execute('UPDATE solutions SET feedback=? WHERE id=?', (request.feedback, request.solution_id))
        self.conn.commit()
        # Retrieve the updated solution
        cursor.execute('SELECT id, post_id, student_id, filename, content, timestamp, grade, feedback FROM solutions WHERE id=?', (request.solution_id,))
        row = cursor.fetchone()
        if row:
            solution = lms_pb2.Solution(
                id=row[0],
                post_id=row[1],
                student_id=row[2],
                filename=row[3],
                content=row[4],
                timestamp=row[5],
                grade=row[6],
                feedback=row[7]
            )
            return lms_pb2.SolutionResponse(solution=solution)
        else:
            if context is not None:
                context.set_details('Solution not found')
                context.set_code(grpc.StatusCode.NOT_FOUND)
            return lms_pb2.SolutionResponse()
    
    def GetAllGrades(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.SolutionList()
        if context is not None:
            # Perform role check
            if payload['role'] != lms_pb2.INSTRUCTOR:
                context.set_details('Permission denied')
                context.set_code(grpc.StatusCode.PERMISSION_DENIED)
                return lms_pb2.SolutionList()
        
        cursor = self.conn.cursor()
        cursor.execute('SELECT id, post_id, student_id, filename, content, timestamp, grade, feedback FROM solutions')
        solutions = []
        for row in cursor.fetchall():
            solution = lms_pb2.Solution(
                id=row[0],
                post_id=row[1],
                student_id=row[2],
                filename=row[3],
                content=row[4],
                timestamp=row[5],
                grade=row[6] if row[6] is not None else 0.0,
                feedback=row[7] if row[7] else ''
            )
            solutions.append(solution)
        return lms_pb2.SolutionList(solutions=solutions)
    
    def generate_llm_response(self, query_id, content):
        try:
            response = "This is a simulated response from the LLM for your query: " + content
            if self.llm_server:
                with grpc.insecure_channel(self.llm_server) as channel:
                    llm_stub = llm_server_pb2_grpc.LLMChatStub(channel)
                    query = llm_server_pb2.QueryRequest(
                        query_id=query_id,
                        message=content
                    )
                    llm_response = llm_stub.Query(query)
                    response = getattr(llm_response, 'message', "No response field found.")
            return response
        except Exception as e:
            print(f"Error communicating with LLM server: {e}")
            return "Failed to generate LLM response."
    
    def generate_and_update_llm_response(self, query_id, content):
        llm_response = self.generate_llm_response(query_id, content)
        print(f"----- Completed {query_id}------")
        try:
            cursor = self.conn.cursor()
            cursor.execute('UPDATE queries SET llm_response=? WHERE id=?', (llm_response, query_id))
            self.conn.commit()
            print(f"LLM response for Query ID {query_id} has been generated and updated.")
        except Exception as e:
            print(f"Error updating LLM response for Query ID {query_id}: {e}")

class RaftServer():
    def __init__(self, sid, self_address, peer_addresses, llm_address):
        self.sid = int(sid)
        self.socket_address = self_address
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.lms_service = LMSService(self.sid, llm_address)

        raft_pb2_grpc.add_RaftServicer_to_server(RaftService(self), self.server)
        lms_pb2_grpc.add_LMSServicer_to_server(self.lms_service, self.server)
        
        self.server.add_insecure_port(self.socket_address)

        # State variables
        self.currentTerm = 0
        self.votedFor = None
        self.state = 'follower'
        self.leaderId = None
        self.peers = [addr for addr in peer_addresses.split(',') if addr != self.socket_address]

        # Log and State Machine
        self.log = []  # List of log entries
        self.commitIndex = 0
        self.lastApplied = 0

        # Persistent state
        self.state_file = f"raft_state_{self.sid}.pkl"
        self.load_state()

        # Initialize nextIndex and matchIndex
        self.nextIndex = {}  # nextIndex for each follower
        self.matchIndex = {}  # matchIndex for each follower

        # Election timer reset event
        self.election_reset_event = threading.Event()
        self.lock = threading.RLock()  # Use reentrant lock to prevent deadlock

        # Condition variable for commit synchronization
        self.commit_cond = threading.Condition(self.lock)

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
                time.sleep(0.1)
            else:
                time.sleep(0.1)

    def election_timer(self):
        while True:
            timeout = random.uniform(0.15, 0.30)
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

                    # Ensure prevLogIndex is within the bounds of self.log
                    if prevLogIndex == 0:
                        prevLogTerm = 0
                    elif prevLogIndex <= len(self.log):
                        prevLogTerm = self.log[prevLogIndex - 1]['term']
                    else:
                        # If prevLogIndex is out of bounds, adjust it
                        prevLogTerm = self.log[-1]['term'] if self.log else 0
                        prevLogIndex = len(self.log)

                    # Prepare entries to send
                    if next_index <= len(self.log):
                        entries = [
                            raft_pb2.LogEntry(term=entry['term'], command=entry['command'])
                            for entry in self.log[next_index - 1:]
                        ]
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
                    # Handle the response as before
                    if response.term > self.currentTerm:
                        with self.lock:
                            self.currentTerm = response.term
                            self.state = 'follower'
                            self.votedFor = None
                            self.save_state()
                        break
                    if response.success:
                        with self.lock:
                            self.matchIndex[peer] = next_index + len(entries) - 1
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
            # Deserialize the command and call the LMS method
            try:
                # The command is a base64 encoded serialized LMS request
                command_bytes = base64.b64decode(command)
                lms_request = pickle.loads(command_bytes)
                method_name = lms_request['method']
                request_message = lms_request['request']
                # Call the LMS method
                lms_method = getattr(self.lms_service, method_name)
                response = lms_method(request_message, None)  # context is None
                print(f"Server {self.sid} applied command: {method_name}")
            except Exception as e:
                print(f"Server {self.sid} failed to apply command: {e}")
        self.save_state()
        # Notify all waiting threads that commitIndex has been updated
        with self.commit_cond:
            self.commit_cond.notify_all()

    def save_state(self):
        with self.lock:
            state = {
                'currentTerm': self.currentTerm,
                'votedFor': self.votedFor,
                'log': self.log,
                'commitIndex': self.commitIndex,
                'lastApplied': self.lastApplied,
            }
            with open(self.state_file, 'wb') as f:
                pickle.dump(state, f)

    def load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'rb') as f:
                state = pickle.load(f)
                self.currentTerm = state['currentTerm']
                self.votedFor = state['votedFor']
                self.log = state['log']
                self.commitIndex = state['commitIndex']
                self.lastApplied = state['lastApplied']
            print(f"Server {self.sid} state loaded.")
        else:
            self.log = []
            self.commitIndex = 0
            self.lastApplied = 0

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
                    leaderId=str(self.server.leaderId) if self.server.leaderId else ""
                )
                print(f"Server {self.server.sid} is not the leader. Redirecting to {self.server.leaderId}")
                return response
            else:
                # Leader: Append the command to the log and start replication
                print(f"Leader {self.server.sid} received ClientRequest.")
                entry = {'term': self.server.currentTerm, 'command': request.command}
                self.server.log.append(entry)
                self.server.save_state()
                entry_index = len(self.server.log)
                # Start replication (send AppendEntries to followers)
                self.server.send_append_entries()
                # Wait for the entry to be committed
                with self.server.commit_cond:
                    while self.server.commitIndex < entry_index:
                        self.server.commit_cond.wait()
                response = raft_pb2.ClientResponseMessage(
                    success=True,
                    message="Command accepted",
                    leaderId=""
                )
                return response

def run_server(sid: str, self: str, peers: str, llm: str=None):
    server = RaftServer(sid, self, peers, llm)
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
