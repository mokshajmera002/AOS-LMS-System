# lms_raft_client_gui.py

import time
import streamlit as st
import grpc
import pandas as pd
import lms_pb2
import lms_pb2_grpc
import raft_pb2
import raft_pb2_grpc
import jwt
from jwt import InvalidTokenError
import base64
import pickle
import threading

# Constants
JWT_SECRET = 'software_project_management'
JWT_ALGORITHM = 'HS256'

class RaftClientServer:
    def __init__(self, servers):
        self.servers = servers  # List of server addresses
        self.leader_id = None
        self.lock = threading.Lock()
        self.token = None  # Authentication token

    def find_leader(self):
        for index, server_address in enumerate(self.servers):
            try:
                with grpc.insecure_channel(server_address) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    # Send a ClientRequest with empty command to probe for leader
                    request = raft_pb2.ClientRequestMessage(command="")
                    response = stub.ClientRequest(request, timeout=1)
                    if response.success:
                        # This server is the leader
                        self.leader_id = index
                        print(f"Leader found: {self.leader_id}")
                        return True
                    else:
                        if response.leaderId:
                            self.leader_id = int(response.leaderId)
                            print(f"Redirecting to leader at: {self.leader_id}")
                            return True
                        else:
                            continue
            except Exception:
                continue
        return False

    def send_command(self, lms_method, lms_request):
        with self.lock:
            if self.leader_id is None:
                found = self.find_leader()
                if not found:
                    print("Leader not found in the cluster.")
                    return None

            try:
                with grpc.insecure_channel(self.servers[self.leader_id]) as channel:
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
                            return self.send_command(lms_method, lms_request)
                        else:
                            self.leader_id = None
                            print("Leader information not provided. Retrying leader discovery.")
                            return self.send_command(lms_method, lms_request)
            except Exception as e:
                print(f"Failed to send command to leader {self.leader_id}: {e}")
                self.leader_id = None
                return self.send_command(lms_method, lms_request)

    def call_lms_method(self, lms_method, lms_request):
        # Directly call the LMS service on the leader to get the response
        try:
            with grpc.insecure_channel(self.servers[self.leader_id]) as channel:
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

def main():
    st.title("LMS System")
    raft_client_server = None

    if 'servers' not in st.session_state:
        st.session_state.servers = None
    if 'token' not in st.session_state:
        st.session_state.token = None
        st.session_state.role = None
        st.session_state.user_id = None

    if st.session_state.servers is None:
        server_input_page()
    else:
        raft_client_server = RaftClientServer(st.session_state.servers)
        if raft_client_server.token:
            st.session_state.token = raft_client_server.token

    if st.session_state.token:
        # User is logged in
        # Decode token to get user role
        try:
            payload = jwt.decode(st.session_state.token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            st.session_state.role = payload['role']
            st.session_state.user_id = payload['user_id']
            raft_client_server.token = st.session_state.token
        except InvalidTokenError:
            st.error("Invalid token.")
            st.session_state.token = None
            raft_client_server.token = None
            st.session_state.role = None
            st.session_state.user_id = None
            st.rerun()
            return

        if st.session_state.role == lms_pb2.STUDENT:
            student_menu(raft_client_server)
        elif st.session_state.role == lms_pb2.INSTRUCTOR:
            instructor_menu(raft_client_server)
        elif st.session_state.role == lms_pb2.ADMIN:
            admin_menu(raft_client_server)
        else:
            st.error("Unknown role.")
    else:
        if raft_client_server:
            login(raft_client_server)

def server_input_page():
    st.subheader("Configure Raft Servers")
    server_addresses = st.text_area("Enter server addresses separated by commas (e.g., 127.0.0.1:50000,127.0.0.1:50001)", height=100, value="127.0.0.1:50000,127.0.0.1:50001,127.0.0.1:50002")
    if st.button("Submit"):
        servers = [addr.strip() for addr in server_addresses.split(',') if addr.strip()]
        if not servers:
            st.error("Please enter at least one server address.")
            return
        # Verify connections
        valid_servers = []
        for server in servers:
            try:
                with grpc.insecure_channel(server) as channel:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    request = raft_pb2.ClientRequestMessage(command="")
                    stub.ClientRequest(request, timeout=2)
                valid_servers.append(server)
                st.success(f"Connected to server: {server}")
            except:
                st.warning(f"Cannot connect to server: {server}")
        if valid_servers:
            st.session_state.servers = valid_servers
            st.success("Server addresses validated and saved.")
            time.sleep(1)
            st.rerun()
        else:
            st.error("No valid servers found. Please check the addresses and try again.")

def login(raft_client_server):
    st.subheader("Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        lms_request = lms_pb2.LoginRequest(username=username, password=password)
        lms_method = 'Login'
        response = raft_client_server.send_command(lms_method, lms_request)
        if response and response.token:
            st.success("Logged in successfully!")
            raft_client_server.token = response.token
            st.session_state.token = response.token
            st.rerun()
        else:
            st.error("Login failed.")

def logout(raft_client_server):
    st.session_state.token = None
    raft_client_server.token = None
    st.session_state.role = None
    st.session_state.user_id = None
    st.success("Logged out.")
    st.rerun()

def student_menu(raft_client_server):
    st.sidebar.title("Student Menu")
    choice = st.sidebar.selectbox("Options", ["View Posts", "Download Post", "Upload Solution",
                                              "View Grades", "Post Query", "View Queries and Replies", "Logout"])
    if choice == "View Posts":
        view_posts(raft_client_server)
    elif choice == "Download Post":
        download_post(raft_client_server)
    elif choice == "Upload Solution":
        upload_solution(raft_client_server)
    elif choice == "View Grades":
        view_grades(raft_client_server)
    elif choice == "Post Query":
        post_query_with_options(raft_client_server)
    elif choice == "View Queries and Replies":
        view_queries_and_replies_student(raft_client_server)
    elif choice == "Logout":
        logout(raft_client_server)

def instructor_menu(raft_client_server):
    st.sidebar.title("Instructor Menu")
    choice = st.sidebar.selectbox("Options", ["Post Content", "View Posts", "Download Student Solution",
                                              "Assign Grade", "Provide Feedback", "View All Students' Grades",
                                              "View Queries and Reply", "Logout"])
    if choice == "Post Content":
        post_content(raft_client_server)
    elif choice == "View Posts":
        view_posts(raft_client_server)
    elif choice == "Download Student Solution":
        download_student_solution(raft_client_server)
    elif choice == "Assign Grade":
        assign_grade(raft_client_server)
    elif choice == "Provide Feedback":
        provide_feedback(raft_client_server)
    elif choice == "View All Students' Grades":
        view_all_students_grades(raft_client_server)
    elif choice == "View Queries and Reply":
        view_queries_and_replies_instructor(raft_client_server)
    elif choice == "Logout":
        logout(raft_client_server)

def admin_menu(raft_client_server):
    st.sidebar.title("Admin Menu")
    choice = st.sidebar.selectbox("Options", ["Create User", "List Users", "Logout"])
    if choice == "Create User":
        create_user(raft_client_server)
    elif choice == "List Users":
        list_users(raft_client_server)
    elif choice == "Logout":
        logout(raft_client_server)

def deduplicate_posts(post_list):
    final_posts = []
    for post in post_list:
        for _ep in final_posts:
            if _ep.title == post.title:
                if lms_pb2.PostType.Name(post.type) == lms_pb2.PostType.Name(_ep.type):
                    break
        else:
            final_posts.append(post)
    return final_posts

# Common Functions
def view_posts(raft_client_server):
    st.subheader("Posts")
    lms_request = lms_pb2.Empty()
    lms_method = 'GetPosts'
    response = raft_client_server.send_command(lms_method, lms_request)
    if response:    
        for post in deduplicate_posts(response.posts):
            post_type = lms_pb2.PostType.Name(post.type)
            with st.expander(f"{post.title} (Type: {post_type})"):
                st.write(f"**Description:** {post.description}")
                st.write(f"**Timestamp:** {post.timestamp}")
    else:
        st.error("Failed to retrieve posts.")

def download_post(raft_client_server):
    st.subheader("Download Post")
    lms_request = lms_pb2.Empty()
    lms_method = 'GetPosts'
    response = raft_client_server.send_command(lms_method, lms_request)
    if response:
        post_options = {f"{post.id}: {post.title}": post.id for post in deduplicate_posts(response.posts)}
        selected_post = st.selectbox("Select Post to Download", list(post_options.keys()))
        if st.button("Download"):
            post_id = post_options[selected_post]
            lms_request = lms_pb2.PostId(id=post_id)
            lms_method = 'DownloadPost'
            post_response = raft_client_server.send_command(lms_method, lms_request)
            if post_response and post_response.post and post_response.post.content:
                st.write(f"**Title:** {post_response.post.title}")
                st.write(f"**Description:** {post_response.post.description}")
                st.download_button(label="Download File",
                                   data=post_response.post.content,
                                   file_name=post_response.post.filename)
            else:
                st.warning("No content to download.")
    else:
        st.error("Failed to retrieve posts.")

def upload_solution(raft_client_server):
    st.subheader("Upload Solution")
    lms_request = lms_pb2.Empty()
    lms_method = 'GetPosts'
    response = raft_client_server.send_command(lms_method, lms_request)
    if response:
        assignment_posts = [post for post in deduplicate_posts(response.posts) if post.type == lms_pb2.ASSIGNMENT]
        post_options = {f"{post.id}: {post.title}": post.id for post in deduplicate_posts(assignment_posts)}
        if not post_options:
            st.info("No assignments available.")
            return
        selected_post = st.selectbox("Select Assignment", list(post_options.keys()))
        uploaded_file = st.file_uploader("Choose a file")
        if st.button("Upload"):
            if uploaded_file is not None:
                post_id = post_options[selected_post]
                content = uploaded_file.read()
                filename = uploaded_file.name
                lms_request = lms_pb2.SolutionRequest(
                    post_id=post_id,
                    student_id=st.session_state.user_id,
                    filename=filename,
                    content=content
                )
                lms_method = 'UploadSolution'
                solution_response = raft_client_server.send_command(lms_method, lms_request)
                if solution_response and solution_response.solution:
                    st.success(f"Uploaded Solution ID: {solution_response.solution.id}")
                else:
                    st.error("Failed to upload solution.")
            else:
                st.warning("Please upload a file.")
    else:
        st.error("Failed to retrieve assignments.")

def view_grades(raft_client_server):
    st.subheader("Your Grades")
    lms_request = lms_pb2.StudentId(id=st.session_state.user_id)
    lms_method = 'ViewGrades'
    response = raft_client_server.send_command(lms_method, lms_request)
    if response:
        if not response.solutions:
            st.info("No grades available.")
        for sol in response.solutions:
            st.write(f"**Assignment {sol.post_id} - Grade: {sol.grade}**")
            if sol.feedback:
                st.write(f"**Feedback:** {sol.feedback}")
    else:
        st.error("Failed to retrieve grades.")

def post_query_with_options(raft_client_server):
    st.subheader("Post Query")
    query_content = st.text_area("Enter your query")
    target_option = st.selectbox("Who do you want to ask?", ["Instructor", "LLM (AI Assistant)"])
    target_mapping = {
        "Instructor": lms_pb2.PROFESSOR,
        "LLM (AI Assistant)": lms_pb2.LLM
    }
    if st.button("Post Query"):
        if query_content.strip():
            lms_request = lms_pb2.QueryRequest(
                student_id=st.session_state.user_id,
                content=query_content,
                target=target_mapping[target_option]
            )
            lms_method = 'PostQuery'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response and response.id:
                if target_option == "LLM (AI Assistant)" and response.llm_response:
                    st.success(f"LLM Response: {response.llm_response}")
                else:
                    st.success(f"Posted Query ID: {response.id}")
            else:
                st.error("Failed to post query.")
        else:
            st.warning("Query cannot be empty.")

def view_queries_and_replies_student(raft_client_server):
    st.subheader("Your Queries and Replies")
    lms_request = lms_pb2.Empty()
    lms_method = 'GetQueries'
    response = raft_client_server.send_command(lms_method, lms_request)
    if response:
        for q in response.queries:
            if q.student_id == st.session_state.user_id:
                with st.expander(f"Query {q.id} (Target: {lms_pb2.QueryTarget.Name(q.target)})"):
                    st.write(q.content)
                    if q.target == lms_pb2.LLM and q.llm_response:
                        st.write(f"**LLM Response:** {q.llm_response}")
                    else:
                        lms_request = lms_pb2.QueryId(id=q.id)
                        lms_method = 'GetReplies'
                        replies_response = raft_client_server.send_command(lms_method, lms_request)
                        if replies_response:
                            for r in replies_response.replies:
                                st.write(f"**Replied by {r.username}:** {r.content}")
    else:
        st.error("Failed to retrieve queries.")

def post_content(raft_client_server):
    st.subheader("Post Content")
    title = st.text_input("Title")
    description = st.text_area("Description")
    type_choice = st.selectbox("Select Post Type", ["Assignment", "Course Material", "Announcement"])
    type_mapping = {
        "Assignment": lms_pb2.ASSIGNMENT,
        "Course Material": lms_pb2.COURSE_MATERIAL,
        "Announcement": lms_pb2.ANNOUNCEMENT
    }
    uploaded_file = st.file_uploader("Choose a file (optional)")
    if st.button("Post"):
        if title.strip() and description.strip():
            filename = ''
            content = b''
            if uploaded_file is not None:
                filename = uploaded_file.name
                content = uploaded_file.read()
            lms_request = lms_pb2.PostRequest(
                title=title,
                description=description,
                type=type_mapping[type_choice],
                filename=filename,
                content=content
            )
            lms_method = 'PostContent'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response and response.post:
                st.success(f"Posted Content ID: {response.post.id}")
            else:
                st.error("Failed to post content.")
        else:
            st.warning("Title and Description cannot be empty.")

def download_student_solution(raft_client_server):
    st.subheader("Download Student Solution")
    lms_request = lms_pb2.Empty()
    lms_method = 'GetPosts'
    response = raft_client_server.send_command(lms_method, lms_request)
    if response:
        assignment_posts = [post for post in deduplicate_posts(response.posts) if post.type == lms_pb2.ASSIGNMENT]
        post_options = {f"{post.id}: {post.title}": post.id for post in deduplicate_posts(assignment_posts)}
        if not post_options:
            st.info("No assignments available.")
            return
        selected_post = st.selectbox("Select Assignment", list(post_options.keys()))
        assignment_id = post_options[selected_post]
        lms_request = lms_pb2.PostId(id=assignment_id)
        lms_method = 'GetSolutions'
        solutions_response = raft_client_server.send_command(lms_method, lms_request)
        if solutions_response:
            sol_options = {f"Solution {sol.id} by Student {sol.student_id}": sol.id for sol in solutions_response.solutions}
            if not sol_options:
                st.info("No solutions available.")
                return
            selected_solution = st.selectbox("Select Solution to Download", list(sol_options.keys()))
            sol_id = sol_options[selected_solution]
            if st.button("Download Solution"):
                lms_request = lms_pb2.PostId(id=sol_id)
                lms_method = 'DownloadSolution'
                solution_response = raft_client_server.send_command(lms_method, lms_request)
                if solution_response and solution_response.solution and solution_response.solution.content:
                    st.download_button(label="Download Solution",
                                       data=solution_response.solution.content,
                                       file_name=solution_response.solution.filename)
                else:
                    st.warning("Solution not found or no content.")
        else:
            st.error("Failed to retrieve solutions.")
    else:
        st.error("Failed to retrieve assignments.")

def assign_grade(raft_client_server):
    st.subheader("Assign Grade")
    lms_request = lms_pb2.Empty()
    lms_method = 'GetPosts'
    response = raft_client_server.send_command(lms_method, lms_request)
    if response:
        assignment_posts = [post for post in deduplicate_posts(response.posts) if post.type == lms_pb2.ASSIGNMENT]
        post_options = {f"{post.id}: {post.title}": post.id for post in deduplicate_posts(assignment_posts)}
        if not post_options:
            st.info("No assignments available.")
            return
        selected_post = st.selectbox("Select Assignment", list(post_options.keys()))
        assignment_id = post_options[selected_post]
        lms_request = lms_pb2.PostId(id=assignment_id)
        lms_method = 'GetSolutions'
        solutions_response = raft_client_server.send_command(lms_method, lms_request)
        if solutions_response:
            sol_options = {f"Solution {sol.id} by Student {sol.student_id}": sol.id for sol in solutions_response.solutions}
            if not sol_options:
                st.info("No solutions available.")
                return
            selected_solution = st.selectbox("Select Solution to Grade", list(sol_options.keys()))
            grade = st.number_input("Enter Grade", min_value=0.0, max_value=100.0, step=0.5)
            if st.button("Assign Grade"):
                sol_id = sol_options[selected_solution]
                lms_request = lms_pb2.GradeRequest(
                    solution_id=sol_id,
                    grade=grade
                )
                lms_method = 'AssignGrade'
                response = raft_client_server.send_command(lms_method, lms_request)
                if response and response.solution:
                    st.success(f"Assigned Grade {response.solution.grade} to Solution {response.solution.id}")
                else:
                    st.error("Failed to assign grade.")
        else:
            st.error("Failed to retrieve solutions.")
    else:
        st.error("Failed to retrieve assignments.")

def provide_feedback(raft_client_server):
    st.subheader("Provide Feedback on Assignments")
    lms_request = lms_pb2.Empty()
    lms_method = 'GetAllGrades'
    response = raft_client_server.send_command(lms_method, lms_request)
    if response:
        sol_options = {f"Solution {sol.id} for Assignment {sol.post_id} by Student {sol.student_id}": sol.id for sol in response.solutions}
        if not sol_options:
            st.info("No solutions available.")
            return
        selected_solution = st.selectbox("Select Solution to Provide Feedback", list(sol_options.keys()))
        feedback_text = st.text_area("Enter Feedback")
        if st.button("Submit Feedback"):
            sol_id = sol_options[selected_solution]
            lms_request = lms_pb2.FeedbackRequest(
                solution_id=sol_id,
                feedback=feedback_text
            )
            lms_method = 'AddFeedback'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response and response.solution:
                st.success(f"Feedback added to Solution {response.solution.id}")
            else:
                st.error("Failed to add feedback.")
    else:
        st.error("Failed to retrieve solutions.")

def view_all_students_grades(raft_client_server):
    st.subheader("All Students' Grades")
    lms_request = lms_pb2.Empty()
    lms_method = 'GetAllGrades'
    response = raft_client_server.send_command(lms_method, lms_request)
    if response:
        if not response.solutions:
            st.info("No grades available.")
        user_data = []
        for sol in response.solutions:
            user_data.append({
                "Solution ID": sol.id,
                "Assignment ID": sol.post_id,
                "Student ID": sol.student_id,
                "Grade": sol.grade,
                "Feedback": sol.feedback if sol.feedback else "N/A"
            })
        df = pd.DataFrame(user_data)
        df.drop(index=df.index[df['Student ID']==0], inplace=True)
        st.dataframe(df)
    else:
        st.error("Failed to retrieve grades.")

def view_queries_and_replies_instructor(raft_client_server):
    st.subheader("Queries and Replies")
    lms_request = lms_pb2.Empty()
    lms_method = 'GetQueries'
    response = raft_client_server.send_command(lms_method, lms_request)
    if response:
        for q in response.queries:
            if q.target == lms_pb2.PROFESSOR:
                with st.expander(f"Query {q.id} by Student {q.student_id}"):
                    st.write(q.content)
                    lms_request = lms_pb2.QueryId(id=q.id)
                    lms_method = 'GetReplies'
                    replies_response = raft_client_server.send_command(lms_method, lms_request)
                    if replies_response:
                        for r in replies_response.replies:
                            st.write(f"**Reply {r.id} by {r.username}:** {r.content}")
                    reply_content = st.text_area(f"Enter reply to Query {q.id}", key=f"reply_{q.id}")
                    if st.button(f"Post Reply to Query {q.id}", key=f"button_{q.id}"):
                        if reply_content.strip():
                            lms_request = lms_pb2.ReplyRequest(
                                query_id=q.id,
                                user_id=st.session_state.user_id,
                                content=reply_content
                            )
                            lms_method = 'PostReply'
                            reply_response = raft_client_server.send_command(lms_method, lms_request)
                            if reply_response and reply_response.id:
                                st.success(f"Posted Reply ID: {reply_response.id}")
                                st.rerun()
                            else:
                                st.error("Failed to post reply.")
                        else:
                            st.warning("Reply cannot be empty.")
    else:
        st.error("Failed to retrieve queries.")

def create_user(raft_client_server):
    st.subheader("Create User")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    role_choice = st.selectbox("Select Role", ["Student", "Instructor"])
    role_mapping = {
        "Student": lms_pb2.STUDENT,
        "Instructor": lms_pb2.INSTRUCTOR
    }
    if st.button("Create User"):
        if username.strip() and password.strip():
            lms_request = lms_pb2.UserRequest(
                username=username,
                password=password,
                role=role_mapping[role_choice]
            )
            lms_method = 'CreateUser'
            response = raft_client_server.send_command(lms_method, lms_request)
            if response and response.user:
                st.success(f"Created User ID: {response.user.id}")
            else:
                st.error("Failed to create user.")
        else:
            st.warning("Username and Password cannot be empty.")

def list_users(raft_client_server):
    st.subheader("List Users")
    lms_request = lms_pb2.Empty()
    lms_method = 'ListUsers'
    response = raft_client_server.send_command(lms_method, lms_request)
    if response:
        user_data = []
        for user in response.users:
            role_name = lms_pb2.UserRole.Name(user.role)
            user_data.append({
                "User ID": user.id,
                "Username": user.username,
                "Role": role_name
            })
        df = pd.DataFrame(user_data)
        st.dataframe(df)
    else:
        st.error("Failed to list users.")

if __name__ == "__main__":
    main()
