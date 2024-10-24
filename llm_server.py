import grpc
import llm_server_pb2
import llm_server_pb2_grpc

from concurrent import futures

from utils import llm

SOCKET_ADDRESS = "localhost:50051"

class LLMServicer(llm_server_pb2_grpc.LLMChatServicer):
    def Query(self, request, context):
        query_id = request.query_id
        query_message = llm.process_query(message=request.message)

        # print("\nquery_id : " + str(query_id) + "\nquery_message : "+query_message)
        print(f"--------Completed---Q{request.query_id}----------")
        return llm_server_pb2.QueryResponse(query_id=query_id, message=query_message)

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))

    llm_server_pb2_grpc.add_LLMChatServicer_to_server(
        server   = server, 
        servicer = LLMServicer()
    )

    server.add_insecure_port(SOCKET_ADDRESS)
    server.start()
    print(f"Server Started : {SOCKET_ADDRESS}")
    
    server.wait_for_termination()

if __name__ == "__main__":
    server()