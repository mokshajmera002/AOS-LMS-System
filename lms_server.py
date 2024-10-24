import os
import time
import grpc
import jwt
import sqlite3
import datetime

from jwt import InvalidTokenError
from concurrent import futures
from datetime import timedelta

import lms_pb2
import lms_pb2_grpc


DATABASE = 'LMS_DATABASE.db'
JWT_SECRET = 'software_project_management'
JWT_ALGORITHM = 'HS256'

class LMSService(lms_pb2_grpc.LMSServicer):
    def __init__(self):
        self.conn = sqlite3.connect(DATABASE, check_same_thread=False)
        self.create_tables()
        self.create_default_admin()

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
                            filepath TEXT,
                            timestamp TEXT
                        )''')
        # Solutions table
        cursor.execute('''CREATE TABLE IF NOT EXISTS solutions (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            post_id INTEGER,
                            student_id INTEGER,
                            filename TEXT,
                            filepath TEXT,
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
                'exp': datetime.datetime.now(datetime.UTC) + timedelta(hours=1)
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
        if payload['role'] != lms_pb2.ADMIN:
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
        if payload['role'] != lms_pb2.ADMIN:
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
        if payload['role'] != lms_pb2.INSTRUCTOR:
            context.set_details('Permission denied')
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return lms_pb2.PostResponse()
        cursor = self.conn.cursor()
        timestamp = datetime.datetime.now(datetime.UTC).isoformat()
        # Save the file if content is provided
        filename = request.filename
        filepath = None
        if request.content:
            post_dir = 'posts'
            os.makedirs(post_dir, exist_ok=True)
            unique_filename = f"{int(time.time()*1000)}_{filename}"
            filepath = os.path.join(post_dir, unique_filename)
            with open(filepath, 'wb') as f:
                f.write(request.content)
        cursor.execute('INSERT INTO posts (title, description, type, filename, filepath, timestamp) VALUES (?, ?, ?, ?, ?, ?)',
                       (request.title, request.description, request.type, filename, filepath, timestamp))
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
        cursor = self.conn.cursor()
        cursor.execute('SELECT id, title, description, type, filename, filepath, timestamp FROM posts WHERE id=?', (request.id,))
        row = cursor.fetchone()
        if row:
            # Read the file content from disk if it exists
            content = b''
            if row[5]:
                try:
                    with open(row[5], 'rb') as f:
                        content = f.read()
                except IOError:
                    context.set_details('File not found')
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    return lms_pb2.PostResponse()
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
            context.set_details('Post not found')
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return lms_pb2.PostResponse()

    def UploadSolution(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.SolutionResponse()
        if payload['role'] != lms_pb2.STUDENT:
            context.set_details('Permission denied')
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return lms_pb2.SolutionResponse()
        cursor = self.conn.cursor()
        timestamp = datetime.datetime.now(datetime.UTC).isoformat()
        # Save the file to disk
        solution_dir = 'solutions'
        os.makedirs(solution_dir, exist_ok=True)
        unique_filename = f"{int(time.time()*1000)}_{request.filename}"
        filepath = os.path.join(solution_dir, unique_filename)
        with open(filepath, 'wb') as f:
            f.write(request.content)
        cursor.execute('''INSERT INTO solutions (post_id, student_id, filename, filepath, timestamp)
                          VALUES (?, ?, ?, ?, ?)''',
                       (request.post_id, payload['user_id'], request.filename, filepath, timestamp))
        self.conn.commit()
        solution_id = cursor.lastrowid
        solution = lms_pb2.Solution(
            id=solution_id,
            post_id=request.post_id,
            student_id=payload['user_id'],
            filename=request.filename,
            timestamp=timestamp,
            grade=0.0
        )
        return lms_pb2.SolutionResponse(solution=solution)

    def GetSolutions(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.SolutionList()
        if payload['role'] != lms_pb2.INSTRUCTOR:
            context.set_details('Permission denied')
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return lms_pb2.SolutionList()
        cursor = self.conn.cursor()
        cursor.execute('''SELECT id, post_id, student_id, filename, filepath, timestamp, grade
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
        if payload['role'] != lms_pb2.INSTRUCTOR:
            context.set_details('Permission denied')
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return lms_pb2.SolutionResponse()
        cursor = self.conn.cursor()
        cursor.execute('SELECT id, post_id, student_id, filename, filepath, timestamp, grade FROM solutions WHERE id=?', (request.id,))
        row = cursor.fetchone()
        if row:
            # Read the file content from disk
            try:
                with open(row[4], 'rb') as f:
                    content = f.read()
            except IOError:
                context.set_details('File not found')
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return lms_pb2.SolutionResponse()
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
            context.set_details('Solution not found')
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return lms_pb2.SolutionResponse()

    def AssignGrade(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.SolutionResponse()
        if payload['role'] != lms_pb2.INSTRUCTOR:
            context.set_details('Permission denied')
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return lms_pb2.SolutionResponse()
        cursor = self.conn.cursor()
        # Update the grade for the specified solution
        cursor.execute('UPDATE solutions SET grade=? WHERE id=?', (request.grade, request.solution_id))
        self.conn.commit()
        # Retrieve the updated solution
        cursor.execute('SELECT id, post_id, student_id, filename, filepath, timestamp, grade FROM solutions WHERE id=?', (request.solution_id,))
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
            context.set_details('Solution not found')
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return lms_pb2.SolutionResponse()

    def ViewGrades(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.SolutionList()
        if payload['role'] != lms_pb2.STUDENT:
            context.set_details('Permission denied')
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return lms_pb2.SolutionList()
        cursor = self.conn.cursor()
        cursor.execute('''SELECT id, post_id, student_id, filename, filepath, timestamp, grade, feedback
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
        if payload['role'] != lms_pb2.STUDENT:
            context.set_details('Permission denied')
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return lms_pb2.Query()
        cursor = self.conn.cursor()
        timestamp = datetime.datetime.now(datetime.UTC).isoformat()

        llm_response = ''
        if request.target == lms_pb2.LLM:
            # Generate dummy LLM response
            llm_response = self.generate_dummy_llm_response(request.content)

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
        if payload['role'] not in [lms_pb2.INSTRUCTOR, lms_pb2.ADMIN]:
            context.set_details('Permission denied')
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return lms_pb2.Reply()
        cursor = self.conn.cursor()
        timestamp = datetime.datetime.now(datetime.UTC).isoformat()
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
            'SELECT replies.id, replies.query_id, replies.user_id, replies.content, replies.timestamp, users.username FROM replies, users WHERE query_id=? and replies.user_id=users.id',
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
        if payload['role'] != lms_pb2.INSTRUCTOR:
            context.set_details('Permission denied')
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return lms_pb2.SolutionResponse()
        cursor = self.conn.cursor()
        # Update the feedback for the specified solution
        cursor.execute('UPDATE solutions SET feedback=? WHERE id=?', (request.feedback, request.solution_id))
        self.conn.commit()
        # Retrieve the updated solution
        cursor.execute('SELECT id, post_id, student_id, filename, filepath, timestamp, grade, feedback FROM solutions WHERE id=?', (request.solution_id,))
        row = cursor.fetchone()
        if row:
            solution = lms_pb2.Solution(
                id=row[0],
                post_id=row[1],
                student_id=row[2],
                filename=row[3],
                timestamp=row[5],
                grade=row[6],
                feedback=row[7]
            )
            return lms_pb2.SolutionResponse(solution=solution)
        else:
            context.set_details('Solution not found')
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return lms_pb2.SolutionResponse()
        
    def GetAllGrades(self, request, context):
        payload = self.authenticate(context)
        if not payload:
            return lms_pb2.SolutionList()
        if payload['role'] != lms_pb2.INSTRUCTOR:
            context.set_details('Permission denied')
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return lms_pb2.SolutionList()
        cursor = self.conn.cursor()
        cursor.execute('SELECT id, post_id, student_id, filename, filepath, timestamp, grade, feedback FROM solutions')
        solutions = []
        for row in cursor.fetchall():
            solution = lms_pb2.Solution(
                id=row[0],
                post_id=row[1],
                student_id=row[2],
                filename=row[3],
                timestamp=row[5],
                grade=row[6] if row[6] is not None else 0.0,
                feedback=row[7] if row[7] else ''
            )
            solutions.append(solution)
        return lms_pb2.SolutionList(solutions=solutions)
    
    def generate_dummy_llm_response(self, content):
        # Temporary dummy function to simulate LLM response
        return "This is a simulated response from the LLM for your query: " + content


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lms_pb2_grpc.add_LMSServicer_to_server(LMSService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print('Server started at port 50051')
    try:
        while True:
            time.sleep(86400)  # One day
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
