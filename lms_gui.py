# lms_gui.py
import streamlit as st
import grpc
import pandas as pd
import lms_pb2
import lms_pb2_grpc
import os
import jwt
from jwt import InvalidTokenError
import base64

# Constants
SERVER_ADDRESS = 'localhost:50051'
JWT_SECRET = 'software_project_management'
JWT_ALGORITHM = 'HS256'

def main():
    st.title("LMS System")

    if 'token' not in st.session_state:
        st.session_state.token = None
        st.session_state.role = None
        st.session_state.user_id = None

    if st.session_state.token:
        # User is logged in
        if st.session_state.role == lms_pb2.STUDENT:
            student_menu()
        elif st.session_state.role == lms_pb2.INSTRUCTOR:
            instructor_menu()
        elif st.session_state.role == lms_pb2.ADMIN:
            admin_menu()
        else:
            st.error("Unknown role.")
    else:
        login()

def login():
    st.subheader("Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        with grpc.insecure_channel(SERVER_ADDRESS) as channel:
            stub = lms_pb2_grpc.LMS_NEWStub(channel)
            try:
                login_response = stub.Login(lms_pb2.LoginRequest(
                    username=username,
                    password=password
                ))
                token = login_response.token
                # Decode token to get user role
                payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
                st.session_state.token = token
                st.session_state.role = payload['role']
                st.session_state.user_id = payload['user_id']
                st.success("Logged in successfully!")
                st.rerun()
            except grpc.RpcError as e:
                st.error(f"Login failed: {e.details()}")
            except InvalidTokenError:
                st.error("Invalid token received.")

def logout():
    st.session_state.token = None
    st.session_state.role = None
    st.session_state.user_id = None
    st.success("Logged out.")
    st.rerun()

def student_menu():
    st.sidebar.title("Student Menu")
    choice = st.sidebar.selectbox("Options", ["View Posts", "Download Post", "Upload Solution",
                                              "View Grades", "Post Query", "View Queries and Replies", "Logout"])
    if choice == "View Posts":
        view_posts()
    elif choice == "Download Post":
        download_post()
    elif choice == "Upload Solution":
        upload_solution()
    elif choice == "View Grades":
        view_grades()
    elif choice == "Post Query":
        post_query_with_options()
    elif choice == "View Queries and Replies":
        view_queries_and_replies_student()
    elif choice == "Logout":
        logout()

def instructor_menu():
    st.sidebar.title("Instructor Menu")
    choice = st.sidebar.selectbox("Options", ["Post Content", "View Posts", "Download Student Solution",
                                              "Assign Grade", "Provide Feedback", "View All Students' Grades",
                                              "View Queries and Reply", "Logout"])
    if choice == "Post Content":
        post_content()
    elif choice == "View Posts":
        view_posts()
    elif choice == "Download Student Solution":
        download_student_solution()
    elif choice == "Assign Grade":
        assign_grade()
    elif choice == "Provide Feedback":
        provide_feedback()
    elif choice == "View All Students' Grades":
        view_all_students_grades()
    elif choice == "View Queries and Reply":
        view_queries_and_replies_instructor()
    elif choice == "Logout":
        logout()

def admin_menu():
    st.sidebar.title("Admin Menu")
    choice = st.sidebar.selectbox("Options", ["Create User", "List Users", "Logout"])
    if choice == "Create User":
        create_user()
    elif choice == "List Users":
        list_users()
    elif choice == "Logout":
        logout()

def get_stub():
    channel = grpc.insecure_channel(SERVER_ADDRESS)
    stub = lms_pb2_grpc.LMS_NEWStub(channel)
    metadata = [('authorization', st.session_state.token)]
    return stub, metadata

# Common Functions
def view_posts():
    st.subheader("Posts")
    stub, metadata = get_stub()
    try:
        posts = stub.GetPosts(lms_pb2.Empty(), metadata=metadata)
        for post in posts.posts:
            post_type = lms_pb2.PostType.Name(post.type)
            with st.expander(f"{post.title} (Type: {post_type})"):
                st.write(f"**Description:** {post.description}")
                st.write(f"**Timestamp:** {post.timestamp}")
    except grpc.RpcError as e:
        st.error(f"Error: {e.details()}")

def download_post():
    st.subheader("Download Post")
    stub, metadata = get_stub()
    try:
        posts = stub.GetPosts(lms_pb2.Empty(), metadata=metadata)
        post_options = {f"{post.id}: {post.title}": post.id for post in posts.posts}
        selected_post = st.selectbox("Select Post to Download", list(post_options.keys()))
        if st.button("Download"):
            post_id = post_options[selected_post]
            post_response = stub.DownloadPost(lms_pb2.PostId(id=post_id), metadata=metadata)
            if post_response.post and post_response.post.content:
                st.write(f"**Title:** {post_response.post.title}")
                st.write(f"**Description:** {post_response.post.description}")
                st.download_button(label="Download File",
                                   data=post_response.post.content,
                                   file_name=post_response.post.filename)
            else:
                st.warning("No content to download.")
    except grpc.RpcError as e:
        st.error(f"Error: {e.details()}")

def upload_solution():
    st.subheader("Upload Solution")
    stub, metadata = get_stub()
    try:
        posts = stub.GetPosts(lms_pb2.Empty(), metadata=metadata)
        assignment_posts = [post for post in posts.posts if post.type == lms_pb2.ASSIGNMENT]
        post_options = {f"{post.id}: {post.title}": post.id for post in assignment_posts}
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
                solution = stub.UploadSolution(lms_pb2.SolutionRequest(
                    post_id=post_id,
                    student_id=st.session_state.user_id,
                    filename=filename,
                    content=content
                ), metadata=metadata)
                st.success(f"Uploaded Solution ID: {solution.solution.id}")
            else:
                st.warning("Please upload a file.")
    except grpc.RpcError as e:
        st.error(f"Error: {e.details()}")

def view_grades():
    st.subheader("Your Grades")
    stub, metadata = get_stub()
    try:
        solutions = stub.ViewGrades(lms_pb2.StudentId(id=st.session_state.user_id), metadata=metadata)
        if not solutions.solutions:
            st.info("No grades available.")
        for sol in solutions.solutions:
            st.write(f"**Assignment {sol.post_id} - Grade: {sol.grade}**")
            if sol.feedback:
                st.write(f"**Feedback:** {sol.feedback}")
    except grpc.RpcError as e:
        st.error(f"Error: {e.details()}")

def post_query_with_options():
    st.subheader("Post Query")
    stub, metadata = get_stub()
    query_content = st.text_area("Enter your query")
    target_option = st.selectbox("Who do you want to ask?", ["Instructor", "LLM (AI Assistant)"])
    target_mapping = {
        "Instructor": lms_pb2.PROFESSOR,
        "LLM (AI Assistant)": lms_pb2.LLM
    }
    if st.button("Post Query"):
        if query_content.strip():
            try:
                query = stub.PostQuery(lms_pb2.QueryRequest(
                    student_id=st.session_state.user_id,
                    content=query_content,
                    target=target_mapping[target_option]
                ), metadata=metadata)
                if target_option == "LLM (AI Assistant)":
                    st.success(f"LLM Response: {query.llm_response}")
                else:
                    st.success(f"Posted Query ID: {query.id}")
            except grpc.RpcError as e:
                st.error(f"Error: {e.details()}")
        else:
            st.warning("Query cannot be empty.")

def view_queries_and_replies_student():
    st.subheader("Your Queries and Replies")
    stub, metadata = get_stub()
    try:
        queries = stub.GetQueries(lms_pb2.Empty(), metadata=metadata)
        for q in queries.queries:
            if q.student_id == st.session_state.user_id:
                with st.expander(f"Query {q.id} (Target: {lms_pb2.QueryTarget.Name(q.target)})"):
                    st.write(q.content)
                    if q.target == lms_pb2.LLM and q.llm_response:
                        st.write(f"**LLM Response:** {q.llm_response}")
                    else:
                        replies = stub.GetReplies(lms_pb2.QueryId(id=q.id), metadata=metadata)
                        for r in replies.replies:
                            st.write(f"**Replied by {r.username}:** {r.content}")
                            # st.write(f"**Reply {r.id} by User {r.username}:** {r.content}")
    except grpc.RpcError as e:
        st.error(f"Error: {e.details()}")

# def post_query():
#     st.subheader("Post Query")
#     stub, metadata = get_stub()
#     query_content = st.text_area("Enter your query")
#     if st.button("Post Query"):
#         if query_content.strip():
#             try:
#                 query = stub.PostQuery(lms_pb2.QueryRequest(
#                     student_id=st.session_state.user_id,
#                     content=query_content
#                 ), metadata=metadata)
#                 st.success(f"Posted Query ID: {query.id}")
#             except grpc.RpcError as e:
#                 st.error(f"Error: {e.details()}")
#         else:
#             st.warning("Query cannot be empty.")

# def view_queries_and_replies():
#     st.subheader("Queries and Replies")
#     stub, metadata = get_stub()
#     try:
#         queries = stub.GetQueries(lms_pb2.Empty(), metadata=metadata)
#         for q in queries.queries:
#             with st.expander(f"Query {q.id} by Student {q.student_id}"):
#                 st.write(q.content)
#                 replies = stub.GetReplies(lms_pb2.QueryId(id=q.id), metadata=metadata)
#                 for r in replies.replies:
#                     st.write(f"**Reply {r.id} by User {r.username}:** {r.content}")
#     except grpc.RpcError as e:
#         st.error(f"Error: {e.details()}")

def post_content():
    st.subheader("Post Content")
    stub, metadata = get_stub()
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
            try:
                post_response = stub.PostContent(lms_pb2.PostRequest(
                    title=title,
                    description=description,
                    type=type_mapping[type_choice],
                    filename=filename,
                    content=content
                ), metadata=metadata)
                st.success(f"Posted Content ID: {post_response.post.id}")
            except grpc.RpcError as e:
                st.error(f"Error: {e.details()}")
        else:
            st.warning("Title and Description cannot be empty.")

def download_student_solution():
    st.subheader("Download Student Solution")
    stub, metadata = get_stub()
    try:
        posts = stub.GetPosts(lms_pb2.Empty(), metadata=metadata)
        assignment_posts = [post for post in posts.posts if post.type == lms_pb2.ASSIGNMENT]
        post_options = {f"{post.id}: {post.title}": post.id for post in assignment_posts}
        if not post_options:
            st.info("No assignments available.")
            return
        selected_post = st.selectbox("Select Assignment", list(post_options.keys()))
        assignment_id = post_options[selected_post]
        solutions = stub.GetSolutions(lms_pb2.PostId(id=assignment_id), metadata=metadata)
        sol_options = {f"Solution {sol.id} by Student {sol.student_id}": sol.id for sol in solutions.solutions}
        if not sol_options:
            st.info("No solutions available.")
            return
        selected_solution = st.selectbox("Select Solution to Download", list(sol_options.keys()))
       
       #new code 4-o
        def generate_download_link(file_content, filename):
            b64 = base64.b64encode(file_content).decode()  # Encode file content to base64
            href = f'<a href="data:file/octet-stream;base64,{b64}" download="{filename}">Click here to download the solution</a>'
            return href

        # Check if the 'Download Solution' button is pressed
        sol_id = sol_options[selected_solution]

        if st.button("Download Solution"):
            # Fetch the solution content from the server
            solution_response = stub.DownloadSolution(lms_pb2.PostId(id=sol_id), metadata=metadata)
            
            # Check if the solution exists and has content
            if solution_response.solution and solution_response.solution.content:
                # Generate the download link
                download_link = generate_download_link(solution_response.solution.content, solution_response.solution.filename)
                
                # Use markdown to render the link which automatically triggers download
                st.markdown(download_link, unsafe_allow_html=True)
            else:
                st.warning("Solution not found or no content.")
       #Old Code
        # if st.button("Download Solution"):
        #     sol_id = sol_options[selected_solution]
        #     solution_response = stub.DownloadSolution(lms_pb2.PostId(id=sol_id), metadata=metadata)
        #     if solution_response.solution and solution_response.solution.content:
        #         st.download_button(label="Download Solution",
        #                            data=solution_response.solution.content,
        #                            file_name=solution_response.solution.filename)
        #     else:
        #         st.warning("Solution not found or no content.")
    except grpc.RpcError as e:
        st.error(f"Error: {e.details()}")

def assign_grade():
    st.subheader("Assign Grade")
    stub, metadata = get_stub()
    try:
        posts = stub.GetPosts(lms_pb2.Empty(), metadata=metadata)
        assignment_posts = [post for post in posts.posts if post.type == lms_pb2.ASSIGNMENT]
        post_options = {f"{post.id}: {post.title}": post.id for post in assignment_posts}
        if not post_options:
            st.info("No assignments available.")
            return
        selected_post = st.selectbox("Select Assignment", list(post_options.keys()))
        assignment_id = post_options[selected_post]
        solutions = stub.GetSolutions(lms_pb2.PostId(id=assignment_id), metadata=metadata)
        sol_options = {f"Solution {sol.id} by Student {sol.student_id}": sol.id for sol in solutions.solutions}
        if not sol_options:
            st.info("No solutions available.")
            return
        selected_solution = st.selectbox("Select Solution to Grade", list(sol_options.keys()))
        grade = st.number_input("Enter Grade", min_value=0.0, max_value=100.0, step=0.5)
        if st.button("Assign Grade"):
            sol_id = sol_options[selected_solution]
            response = stub.AssignGrade(lms_pb2.GradeRequest(
                solution_id=sol_id,
                grade=grade
            ), metadata=metadata)
            st.success(f"Assigned Grade {response.solution.grade} to Solution {response.solution.id}")
    except grpc.RpcError as e:
        st.error(f"Error: {e.details()}")

def provide_feedback():
    st.subheader("Provide Feedback on Assignments")
    stub, metadata = get_stub()
    try:
        # Get all solutions
        solutions = stub.GetAllGrades(lms_pb2.Empty(), metadata=metadata)
        sol_options = {f"Solution {sol.id} for Assignment {sol.post_id} by Student {sol.student_id}": sol.id for sol in solutions.solutions}
        if not sol_options:
            st.info("No solutions available.")
            return
        selected_solution = st.selectbox("Select Solution to Provide Feedback", list(sol_options.keys()))
        feedback_text = st.text_area("Enter Feedback")
        if st.button("Submit Feedback"):
            sol_id = sol_options[selected_solution]
            response = stub.AddFeedback(lms_pb2.FeedbackRequest(
                solution_id=sol_id,
                feedback=feedback_text
            ), metadata=metadata)
            st.success(f"Feedback added to Solution {response.solution.id}")
    except grpc.RpcError as e:
        st.error(f"Error: {e.details()}")

def view_all_students_grades():
    st.subheader("All Students' Grades")
    stub, metadata = get_stub()
    try:
        solutions = stub.GetAllGrades(lms_pb2.Empty(), metadata=metadata)
        if not solutions.solutions:
            st.info("No grades available.")
        for sol in solutions.solutions:
            st.write(f"**Student {sol.student_id} - Assignment {sol.post_id} - Grade: {sol.grade}**")
            if sol.feedback:
                st.write(f"**Feedback:** {sol.feedback}")
    except grpc.RpcError as e:
        st.error(f"Error: {e.details()}")

def view_queries_and_replies_instructor():
    st.subheader("Queries and Replies")
    stub, metadata = get_stub()
    try:
        queries = stub.GetQueries(lms_pb2.Empty(), metadata=metadata)
        for q in queries.queries:
            if q.target == lms_pb2.PROFESSOR:
                with st.expander(f"Query {q.id} by Student {q.student_id}"):
                    st.write(q.content)
                    replies = stub.GetReplies(lms_pb2.QueryId(id=q.id), metadata=metadata)
                    for r in replies.replies:
                        st.write(f"**Reply {r.id} by User {r.user_id}:** {r.content}")
                    reply_content = st.text_area(f"Enter reply to Query {q.id}", key=f"reply_{q.id}")
                    if st.button(f"Post Reply to Query {q.id}", key=f"button_{q.id}"):
                        if reply_content.strip():
                            reply = stub.PostReply(lms_pb2.ReplyRequest(
                                query_id=q.id,
                                user_id=st.session_state.user_id,
                                content=reply_content
                            ), metadata=metadata)
                            st.success(f"Posted Reply ID: {reply.id}")
                            # st.rerun()
                        else:
                            st.warning("Reply cannot be empty.")
    except grpc.RpcError as e:
        st.error(f"Error: {e.details()}")



# def view_queries_and_replies_instructor():
#     st.subheader("Queries and Replies")
#     stub, metadata = get_stub()
#     try:
#         queries = stub.GetQueries(lms_pb2.Empty(), metadata=metadata)
#         for q in queries.queries:
#             with st.expander(f"Query {q.id} by Student {q.student_id}"):
#                 st.write(q.content)
#                 replies = stub.GetReplies(lms_pb2.QueryId(id=q.id), metadata=metadata)
#                 for r in replies.replies:
#                     st.write(f"**Reply {r.id} by User {r.user_id}:** {r.content}")
#                 reply_content = st.text_area(f"Enter reply to Query {q.id}", key=f"reply_{q.id}")
#                 if st.button(f"Post Reply to Query {q.id}", key=f"button_{q.id}"):
#                     if reply_content.strip():
#                         reply = stub.PostReply(lms_pb2.ReplyRequest(
#                             query_id=q.id,
#                             user_id=st.session_state.user_id,
#                             content=reply_content
#                         ), metadata=metadata)
#                         st.success(f"Posted Reply ID: {reply.id}")
#                         # st.rerun()
#                     else:
#                         st.warning("Reply cannot be empty.")
#     except grpc.RpcError as e:
#         st.error(f"Error: {e.details()}")

def create_user():
    st.subheader("Create User")
    stub, metadata = get_stub()
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    role_choice = st.selectbox("Select Role", ["Student", "Instructor"])
    role_mapping = {
        "Student": lms_pb2.STUDENT,
        "Instructor": lms_pb2.INSTRUCTOR
    }
    if st.button("Create User"):
        if username.strip() and password.strip():
            try:
                user_response = stub.CreateUser(lms_pb2.UserRequest(
                    username=username,
                    password=password,
                    role=role_mapping[role_choice]
                ), metadata=metadata)
                st.success(f"Created User ID: {user_response.user.id}")
            except grpc.RpcError as e:
                st.error(f"Error: {e.details()}")
        else:
            st.warning("Username and Password cannot be empty.")

def list_users():
    st.subheader("List Users")
    stub, metadata = get_stub()
    try:
        users = stub.ListUsers(lms_pb2.Empty(), metadata=metadata)
        
        # Create a list to hold user data
        user_data = []

        # Collect user information
        for user in users.users:
            role_name = lms_pb2.UserRole.Name(user.role)
            user_data.append({
                "User ID": user.id,
                "Username": user.username,
                "Role": role_name
            })

        # Create a DataFrame from the list
        df = pd.DataFrame(user_data)

        # Create HTML table with custom styles
        html = "<table class='data'>" \
               "<thead><tr><th>User ID</th><th>Username</th><th>Role</th></tr></thead>" \
               "<tbody>"

        for _, row in df.iterrows():
            html += f"<tr><td>{row['User ID']}</td><td>{row['Username']}</td><td>{row['Role']}</td></tr>"

        html += "</tbody></table>"

        # Custom CSS to style the table
        custom_css = """
        <style>
            .data {
                width: 100%;
                border-collapse: collapse;
                text-align: center;
            }
            .data th {
                font-weight: bold;
                background-color: #f2f2f2;
            }
            .data td {
                padding: 8px;
                border: 1px solid #ddd;
            }
        </style>
        """

        # Render the HTML with custom CSS
        st.markdown(custom_css + html, unsafe_allow_html=True)


        # Display the DataFrame as a table
        # st.dataframe(df,use_container_width=True)  # or st.table(df) for a static table

    except grpc.RpcError as e:
        st.error(f"Error: {e.details()}")


if __name__ == "__main__":
    main()
