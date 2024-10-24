import os
import grpc
import lms_pb2
import lms_pb2_grpc

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = lms_pb2_grpc.LMS_NEWStub(channel)
        # Login
        username = input("Enter username: ")
        password = input("Enter password: ")
        try:
            login_response = stub.Login(lms_pb2.LoginRequest(
                username=username,
                password=password
            ))
            token = login_response.token
            metadata = [('authorization', token)]
            # Decode token to get user role
            import jwt
            JWT_SECRET = 'software_project_management'
            JWT_ALGORITHM = 'HS256'
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            role = payload['role']
            user_id = payload['user_id']
        except grpc.RpcError as e:
            print(f"Login failed: {e.details()}")
            return
        except jwt.InvalidTokenError:
            print("Invalid token received.")
            return

        if role == lms_pb2.STUDENT:
            student_menu(stub, metadata, user_id)
        elif role == lms_pb2.INSTRUCTOR:
            instructor_menu(stub, metadata, user_id)
        elif role == lms_pb2.ADMIN:
            admin_menu(stub, metadata, user_id)
        else:
            print("Unknown role.")

def student_menu(stub, metadata, user_id):
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
            # View Posts
            try:
                posts = stub.GetPosts(lms_pb2.Empty(), metadata=metadata)
                print("Posts:")
                for post in posts.posts:
                    post_type = lms_pb2.PostType.Name(post.type)
                    print(f"{post.id}: {post.title} - {post.description} - Type: {post_type}")
            except grpc.RpcError as e:
                print(f"Error: {e.details()}")
        elif choice == '2':
            # Download Post
            try:
                post_id = int(input("Enter Post ID to download: "))
            except ValueError:
                print("Invalid Post ID.")
                continue
            try:
                post_response = stub.DownloadPost(lms_pb2.PostId(id=post_id), metadata=metadata)
                if post_response.post:
                    filename = post_response.post.filename
                    content = post_response.post.content
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
                    print("Post not found.")
            except grpc.RpcError as e:
                print(f"Error: {e.details()}")
        elif choice == '3':
            # Upload Solution
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
            try:
                solution = stub.UploadSolution(lms_pb2.SolutionRequest(
                    post_id=post_id,
                    student_id=user_id,
                    filename=filename,
                    content=content
                ), metadata=metadata)
                print(f"Uploaded Solution ID: {solution.solution.id}")
            except grpc.RpcError as e:
                print(f"Error: {e.details()}")
        elif choice == '4':
            # View Grades
            try:
                solutions = stub.ViewGrades(lms_pb2.StudentId(id=user_id), metadata=metadata)
                print("Your Grades:")
                for sol in solutions.solutions:
                    print(f"Assignment {sol.post_id} - Grade: {sol.grade}")
            except grpc.RpcError as e:
                print(f"Error: {e.details()}")
        elif choice == '5':
            # Post Query
            query_content = input("Enter your query: ")
            if not query_content.strip():
                print("Query cannot be empty.")
                continue
            try:
                query = stub.PostQuery(lms_pb2.QueryRequest(
                    student_id=user_id,
                    content=query_content
                ), metadata=metadata)
                print(f"Posted Query ID: {query.id}")
            except grpc.RpcError as e:
                print(f"Error: {e.details()}")
        elif choice == '6':
            # View Queries and Replies
            try:
                queries = stub.GetQueries(lms_pb2.Empty(), metadata=metadata)
                print("Queries:")
                for q in queries.queries:
                    print(f"Query {q.id} by Student {q.student_id}: {q.content}")
                    replies = stub.GetReplies(lms_pb2.QueryId(id=q.id), metadata=metadata)
                    for r in replies.replies:
                        print(f"  Reply {r.id} by User {r.user_id}: {r.content}")
            except grpc.RpcError as e:
                print(f"Error: {e.details()}")
        elif choice == '7':
            print("Logged out.")
            break
        else:
            print("Invalid choice.")

def instructor_menu(stub, metadata, user_id):
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
            # Post Content
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
            try:
                post_response = stub.PostContent(lms_pb2.PostRequest(
                    title=title,
                    description=description,
                    type=type_choice,
                    filename=filename,
                    content=content
                ), metadata=metadata)
                print(f"Posted Content ID: {post_response.post.id}")
            except grpc.RpcError as e:
                print(f"Error: {e.details()}")
        elif choice == '2':
            # View Posts
            try:
                posts = stub.GetPosts(lms_pb2.Empty(), metadata=metadata)
                print("Posts:")
                for post in posts.posts:
                    post_type = lms_pb2.PostType.Name(post.type)
                    print(f"{post.id}: {post.title} - {post.description} - Type: {post_type}")
            except grpc.RpcError as e:
                print(f"Error: {e.details()}")
        elif choice == '3':
            # Download Student Solution
            try:
                assignment_id = int(input("Enter Assignment ID to view solutions: "))
            except ValueError:
                print("Invalid Assignment ID.")
                continue
            try:
                solutions = stub.GetSolutions(lms_pb2.PostId(id=assignment_id), metadata=metadata)
                for sol in solutions.solutions:
                    print(f"Solution {sol.id} by Student {sol.student_id} - Grade: {sol.grade}")
                    download = input(f"Do you want to download Solution {sol.id}? (y/n): ")
                    if download.lower() == 'y':
                        # Download the solution
                        solution_response = stub.DownloadSolution(lms_pb2.PostId(id=sol.id), metadata=metadata)
                        if solution_response.solution:
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
            except grpc.RpcError as e:
                print(f"Error: {e.details()}")
        elif choice == '4':
            # Assign Grade
            try:
                solution_id = int(input("Enter Solution ID to grade: "))
                grade = float(input("Enter grade: "))
            except ValueError:
                print("Invalid input.")
                continue
            try:
                response = stub.AssignGrade(lms_pb2.GradeRequest(
                    solution_id=solution_id,
                    grade=grade
                ), metadata=metadata)
                print(f"Assigned Grade {response.solution.grade} to Solution {response.solution.id}")
            except grpc.RpcError as e:
                print(f"Error: {e.details()}")
        elif choice == '5':
            # View Queries and Reply
            try:
                queries = stub.GetQueries(lms_pb2.Empty(), metadata=metadata)
                print("Queries:")
                for q in queries.queries:
                    print(f"Query {q.id} by Student {q.student_id}: {q.content}")
                    replies = stub.GetReplies(lms_pb2.QueryId(id=q.id), metadata=metadata)
                    for r in replies.replies:
                        print(f"  Reply {r.id} by User {r.user_id}: {r.content}")
                    reply_content = input(f"Enter reply to Query {q.id} (leave empty to skip): ")
                    if reply_content:
                        reply = stub.PostReply(lms_pb2.ReplyRequest(
                            query_id=q.id,
                            user_id=user_id,
                            content=reply_content
                        ), metadata=metadata)
                        print(f"Posted Reply ID: {reply.id}")
            except grpc.RpcError as e:
                print(f"Error: {e.details()}")
        elif choice == '6':
            print("Logged out.")
            break
        else:
            print("Invalid choice.")

def admin_menu(stub, metadata, user_id):
    while True:
        print("\nAdmin Menu:")
        print("1. Create User")
        print("2. List Users")
        print("3. Logout")
        choice = input("Enter choice: ")
        if choice == '1':
            # Create User
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
            try:
                user_response = stub.CreateUser(lms_pb2.UserRequest(
                    username=username,
                    password=password,
                    role=role_choice
                ), metadata=metadata)
                print(f"Created User ID: {user_response.user.id}")
            except grpc.RpcError as e:
                print(f"Error: {e.details()}")
        elif choice == '2':
            # List Users
            try:
                users = stub.ListUsers(lms_pb2.Empty(), metadata=metadata)
                print("Users:")
                for user in users.users:
                    role_name = lms_pb2.UserRole.Name(user.role)
                    print(f"{user.id}: {user.username} - Role: {role_name}")
            except grpc.RpcError as e:
                print(f"Error: {e.details()}")
        elif choice == '3':
            print("Logged out.")
            break
        else:
            print("Invalid choice.")

if __name__ == '__main__':
    run()
