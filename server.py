# Server.py
import grpc
from concurrent import futures
import mapreduce_pb2_grpc
from servicer import KMeansServiceServicer

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_KMeansServiceServicer_to_server(KMeansServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server running on port 50051...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
