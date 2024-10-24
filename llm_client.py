import os
import grpc

import llm_server_pb2
import llm_server_pb2_grpc

def test_llm_server():
    with grpc.insecure_channel('localhost:50051') as channel:
        llm_stub = llm_server_pb2_grpc.LLMChatStub(channel)
        query = llm_server_pb2.QueryRequest(
            query_id = 1,
            message="What is chlorophyll?"
        )
        response = llm_stub.Query(query)
        print(response)
        
if __name__ == "__main__":
    test_llm_server()