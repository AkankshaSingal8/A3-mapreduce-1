# Master.py
import grpc
from concurrent import futures
import mapreduce_pb2_grpc
import mapreduce_pb2
import numpy as np

class Master:
    def __init__(self, data_file, num_mappers, num_reducers):
        self.data_filepath = data_file
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.centroids = np.random.rand(3, 2)  # Random centroids for example
        self.mapper_stubs = []
        self.reducer_stubs = []

    def connect_workers(self):
        # Assuming workers are running on different ports on localhost for simplicity
        for i in range(self.num_mappers):
            channel = grpc.insecure_channel(f'localhost:{50051+i}')
            self.mapper_stubs.append(mapreduce_pb2_grpc.KMeansServiceStub(channel))
        
        for j in range(self.num_reducers):
            channel = grpc.insecure_channel(f'localhost:{50060+j}')
            self.reducer_stubs.append(mapreduce_pb2_grpc.KMeansServiceStub(channel))

    def distribute_map_tasks(self, data_chunks):
        # Distribute data chunks to mappers
        for i, stub in enumerate(self.mapper_stubs):
            response = stub.ProcessMap(mapreduce_pb2.TaskRequest(
                centroids=self.centroids.flatten().tolist(),
                dataChunk=data_chunks[i],
                numReducers=self.num_reducers
            ))
            print(f"Mapper {i} response: {response.message}")

    def collect_and_distribute_reduce_tasks(self):
        # Collect mapper outputs and distribute them to reducers
        # Placeholder for collection and distribution logic
        pass

    def update_centroids(self):
        # Collect outputs from reducers and update centroids
        # Placeholder for update logic
        pass

    def run(self):
        self.connect_workers()
        data_chunks = ["chunk1", "chunk2", "chunk3"]  # Example chunks
        self.distribute_map_tasks(data_chunks)
        self.collect_and_distribute_reduce_tasks()
        self.update_centroids()

if __name__ == '__main__':
    master = Master(data_file = None, num_mappers=3, num_reducers=2)
    
    master.run()
