import fire
import grpc
import raft_pb2
import raft_pb2_grpc
import lms_pb2
import lms_pb2_grpc
import threading
import pickle
import base64
import jwt
import time
import sys
import os

JWT_SECRET = 'software_project_management'
JWT_ALGORITHM = 'HS256'

class RaftClientServer:
    def __init__(self, servers):
        self.servers = servers  # List of server addresses
        self.leader_id = None
        self.lock = threading.Lock()
        self.token = None  # Authentication token

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
            except Exception:
                continue
        return False

    def send_command(self, lms_method, lms_request):
        with self.lock:
            retries = 3
            while retries > 0:
                if self.leader_id is None:
                    found = self.find_leader()
                    if not found:
                        print("Leader not found in the cluster.")
                        time.sleep(1)
                        retries -= 1
                        continue

                try:
                    with grpc.insecure_channel(self.leader_id) as channel:
                        raft_stub = raft_pb2_grpc.RaftStub(channel)
                        # Serialize the LMS request
                        command_data = {
                            'method': lms_method,
                            'request': lms_request
                        }
                        command_bytes = pickle.dumps(command_data)
                        command_str = base64.b64encode(command_bytes).decode('utf-8')
                        raft_request = raft_pb2.ClientRequestMessage(command=command_str)
                        raft_response = raft_stub.ClientRequest(raft_request, timeout=5)
                        if raft_response.success:
                            print(f"Command '{lms_method}' successfully replicated to the cluster.")
                            # After replication, directly call the LMS method on the leader
                            lms_response = self.call_lms_method(lms_method, lms_request)
                            return lms_response
                        else:
                            if raft_response.leaderId:
                                self.leader_id = raft_response.leaderId
                                print(f"Leader changed to: {self.leader_id}. Retrying command.")
                            else:
                                self.leader_id = None
                                print("Leader information not provided. Retrying leader discovery.")
                    retries -= 1
                except grpc.RpcError as e:
                    print(f"Failed to send command to leader {self.leader_id}: {e}")
                    self.leader_id = None
                    retries -= 1
                except Exception as e:
                    print(f"Unexpected error: {e}")
                    self.leader_id = None
                    retries -= 1
            print("Exceeded maximum retries. Command failed.")
            return None

    def call_lms_method(self, lms_method, lms_request):
        # Directly call the LMS service on the leader to get the response
        try:
            with grpc.insecure_channel(self.leader_id) as channel:
                stub = lms_pb2_grpc.LMSStub(channel)
                # Prepare metadata if token is available
                metadata = []
                if self.token:
                    metadata = [('authorization', self.token)]
                lms_method_func = getattr(stub, lms_method)
                response = lms_method_func(lms_request, timeout=5, metadata=metadata)
                return response
        except Exception as e:
            print(f"Failed to get LMS response from leader {self.leader_id}: {e}")
            return None

def run_raft_client(servers:str):
    servers = [addr for addr in servers.split(',')]
    raft_client_server = RaftClientServer(servers)

    while True:
        try:
            print("\nMain Menu:")
            print("1. Login")
            print("2. Exit")
            choice = input("Enter choice: ")
            if choice == '1':
                username = input("Enter username: ")
                password = input("Enter password: ")
                lms_request = lms_pb2.LoginRequest(username=username, password=password)
                lms_method = 'Login'
                response = raft_client_server.send_command(lms_method, lms_request)
                if response and response.token:
                    print("Login successful.")
                    raft_client_server.token = response.token
                    # Decode token to get user role
                    payload = jwt.decode(response.token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
                    role = payload['role']
                    user_id = payload['user_id']
                    if role == lms_pb2.STUDENT:
                        student_menu(raft_client_server, user_id)
                    elif role == lms_pb2.INSTRUCTOR:
                        instructor_menu(raft_client_server, user_id)
                    elif role == lms_pb2.ADMIN:
                        admin_menu(raft_client_server, user_id)
                    else:
                        print("Unknown role.")
                else:
                    print("Login failed.")
            elif choice == '2':
                print("Exiting Raft Client.")
                break
            else:
                print("Invalid choice.")
        except KeyboardInterrupt:
            print("\nExiting Raft Client.")
            break

def student_menu(raft_client_server, user_id):
    while True:
        print("\nStudent Menu:")
        print("1. View Posts")
        print("2. Download Post")
        print("3. Upload Solution")
        print("4. View Grades")
        print("5. Post Query")
        print("6. View Queries and Replies")
        print("7. Logout")
        choice = input("Enter choice: ")
        if choice == '1':
            lms_request = lms_pb2.Empty()
            lms_method = 'GetPosts'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response:
                print("Posts:")
                for post in response.posts:
                    post_type = lms_pb2.PostType.Name(post.type)
                    print(f"{post.id}: {post.title} - {post.description} - Type: {post_type}")
            else:
                print("Failed to retrieve posts.")
        elif choice == '2':
            try:
                post_id = int(input("Enter Post ID to download: "))
            except ValueError:
                print("Invalid Post ID.")
                continue
            lms_request = lms_pb2.PostId(id=post_id)
            lms_method = 'DownloadPost'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response and response.post:
                filename = response.post.filename
                content = response.post.content
                if content:
                    download_dir = 'downloads'
                    os.makedirs(download_dir, exist_ok=True)
                    filepath = os.path.join(download_dir, filename)
                    with open(filepath, 'wb') as f:
                        f.write(content)
                    print(f"Downloaded Post to {filepath}")
                else:
                    print("No content to download.")
            else:
                print("Failed to download post.")
        elif choice == '3':
            try:
                post_id = int(input("Enter Assignment Post ID: "))
            except ValueError:
                print("Invalid Assignment ID.")
                continue
            filepath = input("Enter the path to your solution file: ")
            if not os.path.exists(filepath):
                print("File not found.")
                continue
            filename = os.path.basename(filepath)
            with open(filepath, 'rb') as f:
                content = f.read()
            lms_request = lms_pb2.SolutionRequest(
                post_id=post_id,
                student_id=user_id,
                filename=filename,
                content=content
            )
            lms_method = 'UploadSolution'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response and response.solution:
                print(f"Uploaded Solution ID: {response.solution.id}")
            else:
                print("Failed to upload solution.")
        elif choice == '4':
            lms_request = lms_pb2.StudentId(id=user_id)
            lms_method = 'ViewGrades'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response:
                print("Your Grades:")
                for sol in response.solutions:
                    print(f"Assignment {sol.post_id} - Grade: {sol.grade}")
            else:
                print("Failed to retrieve grades.")
        elif choice == '5':
            query_content = input("Enter your query: ")
            if not query_content.strip():
                print("Query cannot be empty.")
                continue
            target_choice = input("Target LLM? (y/n): ")
            if target_choice.lower() == 'y':
                target = lms_pb2.LLM
            else:
                target = lms_pb2.PROFESSOR
            lms_request = lms_pb2.QueryRequest(
                student_id=user_id,
                content=query_content,
                target=target
            )
            lms_method = 'PostQuery'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response and response.id:
                print(f"Posted Query ID: {response.id}")
            else:
                print("Failed to post query.")
        elif choice == '6':
            lms_request = lms_pb2.Empty()
            lms_method = 'GetQueries'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response:
                print("Queries:")
                for q in response.queries:
                    print(f"Query {q.id} by Student {q.student_id}: {q.content}")
                    lms_request = lms_pb2.QueryId(id=q.id)
                    lms_method = 'GetReplies'
                    replies_response = raft_client_server.send_command(lms_method, lms_request)
                    if replies_response:
                        for r in replies_response.replies:
                            print(f"  Reply {r.id} by User {r.user_id}: {r.content}")
            else:
                print("Failed to retrieve queries.")
        elif choice == '7':
            print("Logged out.")
            raft_client_server.token = None
            break
        else:
            print("Invalid choice.")

def instructor_menu(raft_client_server, user_id):
    while True:
        print("\nInstructor Menu:")
        print("1. Post Content")
        print("2. View Posts")
        print("3. Download Student Solution")
        print("4. Assign Grade")
        print("5. View Queries and Reply")
        print("6. Logout")
        choice = input("Enter choice: ")
        if choice == '1':
            title = input("Enter post title: ")
            description = input("Enter post description: ")
            print("Select post type:")
            print("0: Assignment")
            print("1: Course Material")
            print("2: Announcement")
            try:
                type_choice = int(input("Enter type (0/1/2): "))
            except ValueError:
                print("Invalid type.")
                continue
            if type_choice not in [0, 1, 2]:
                print("Invalid type selected.")
                continue
            filename = ''
            content = b''
            filepath = input("Enter the path to the content file (leave empty if none): ")
            if filepath:
                if not os.path.exists(filepath):
                    print("File not found.")
                    continue
                filename = os.path.basename(filepath)
                with open(filepath, 'rb') as f:
                    content = f.read()
            lms_request = lms_pb2.PostRequest(
                title=title,
                description=description,
                type=type_choice,
                filename=filename,
                content=content
            )
            lms_method = 'PostContent'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response and response.post:
                print(f"Posted Content ID: {response.post.id}")
            else:
                print("Failed to post content.")
        elif choice == '2':
            lms_request = lms_pb2.Empty()
            lms_method = 'GetPosts'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response:
                print("Posts:")
                for post in response.posts:
                    post_type = lms_pb2.PostType.Name(post.type)
                    print(f"{post.id}: {post.title} - {post.description} - Type: {post_type}")
            else:
                print("Failed to retrieve posts.")
        elif choice == '3':
            try:
                assignment_id = int(input("Enter Assignment ID to view solutions: "))
            except ValueError:
                print("Invalid Assignment ID.")
                continue
            lms_request = lms_pb2.PostId(id=assignment_id)
            lms_method = 'GetSolutions'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response:
                for sol in response.solutions:
                    print(f"Solution {sol.id} by Student {sol.student_id} - Grade: {sol.grade}")
                    download = input(f"Do you want to download Solution {sol.id}? (y/n): ")
                    if download.lower() == 'y':
                        lms_request = lms_pb2.PostId(id=sol.id)
                        lms_method = 'DownloadSolution'
                        solution_response = raft_client_server.send_command(lms_method, lms_request)
                        if solution_response and solution_response.solution:
                            filename = solution_response.solution.filename
                            content = solution_response.solution.content
                            download_dir = 'downloaded_solutions'
                            os.makedirs(download_dir, exist_ok=True)
                            filepath = os.path.join(download_dir, f"{sol.student_id}_{filename}")
                            with open(filepath, 'wb') as f:
                                f.write(content)
                            print(f"Downloaded Solution to {filepath}")
                        else:
                            print("Solution not found.")
            else:
                print("Failed to retrieve solutions.")
        elif choice == '4':
            try:
                solution_id = int(input("Enter Solution ID to grade: "))
                grade = float(input("Enter grade: "))
            except ValueError:
                print("Invalid input.")
                continue
            lms_request = lms_pb2.GradeRequest(
                solution_id=solution_id,
                grade=grade
            )
            lms_method = 'AssignGrade'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response and response.solution:
                print(f"Assigned Grade {response.solution.grade} to Solution {response.solution.id}")
            else:
                print("Failed to assign grade.")
        elif choice == '5':
            lms_request = lms_pb2.Empty()
            lms_method = 'GetQueries'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response:
                print("Queries:")
                for q in response.queries:
                    print(f"Query {q.id} by Student {q.student_id}: {q.content}")
                    lms_request = lms_pb2.QueryId(id=q.id)
                    lms_method = 'GetReplies'
                    replies_response = raft_client_server.send_command(lms_method, lms_request)
                    if replies_response:
                        for r in replies_response.replies:
                            print(f"  Reply {r.id} by User {r.user_id}: {r.content}")
                    reply_content = input(f"Enter reply to Query {q.id} (leave empty to skip): ")
                    if reply_content:
                        lms_request = lms_pb2.ReplyRequest(
                            query_id=q.id,
                            user_id=user_id,
                            content=reply_content
                        )
                        lms_method = 'PostReply'
                        reply_response = raft_client_server.send_command(lms_method, lms_request)
                        if reply_response and reply_response.id:
                            print(f"Posted Reply ID: {reply_response.id}")
                        else:
                            print("Failed to post reply.")
            else:
                print("Failed to retrieve queries.")
        elif choice == '6':
            print("Logged out.")
            raft_client_server.token = None
            break
        else:
            print("Invalid choice.")

def admin_menu(raft_client_server, user_id):
    while True:
        print("\nAdmin Menu:")
        print("1. Create User")
        print("2. List Users")
        print("3. Logout")
        choice = input("Enter choice: ")
        if choice == '1':
            username = input("Enter username: ")
            password = input("Enter password: ")
            print("Select role:")
            print("0: Student")
            print("1: Instructor")
            try:
                role_choice = int(input("Enter role (0/1): "))
            except ValueError:
                print("Invalid role.")
                continue
            if role_choice not in [0, 1]:
                print("Invalid role selected.")
                continue
            lms_request = lms_pb2.UserRequest(
                username=username,
                password=password,
                role=role_choice
            )
            lms_method = 'CreateUser'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response and response.user:
                print(f"Created User ID: {response.user.id}")
            else:
                print("Failed to create user.")
        elif choice == '2':
            lms_request = lms_pb2.Empty()
            lms_method = 'ListUsers'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response:
                print("Users:")
                for user in response.users:
                    role_name = lms_pb2.UserRole.Name(user.role)
                    print(f"{user.id}: {user.username} - Role: {role_name}")
            else:
                print("Failed to list users.")
        elif choice == '3':
            print("Logged out.")
            raft_client_server.token = None
            break
        else:
            print("Invalid choice.")

if __name__ == '__main__':
    try:
        fire.Fire(run_raft_client)
    except Exception as e:
        print(e)
