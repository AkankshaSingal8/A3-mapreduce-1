import os
import sys
import grpc
import time
import numpy as np
from concurrent import futures
import threading

import worker_pb2 as worker
import worker_pb2_grpc as worker_grpc

p_red = 1
p_map = 1

class Worker(worker_grpc.WorkerServicer):
    def __init__(self):
        super().__init__()
        self.driver_port = '4000'
        self.partition_dict = {}
        self.number_mappers = None
        self.number_reducers = None
        self.mapper_ports = []
        self.reducer_ports = []

    def setDriverPort(self, request, context):
        print("Old driver port", self.driver_port)
        self.driver_port = request.port
        print("New driver port", self.driver_port)
        return worker.status(code=200, msg="OK")

    def die(self, request, context):
        return worker.empty()

    def sendPartitionedData(self, request, context):
        print("Sending data")
        reducer_id = request.reducerID
        data = self.partition_dict[reducer_id]
        mapper_data = worker.MapperDataList(data=data)
        print("Data sent")
        return mapper_data

    def partition(self, clusters, num_reducers, map_id):
        print("[!] [MAPPER] Partitioning clusters...")
        map_dir = f"map_output_{map_id}"
        os.makedirs(map_dir, exist_ok=True)
        
        partitions = {i: [] for i in range(num_reducers)}
        partition_counts = {i: 0 for i in range(num_reducers)}

        total_points = sum(len(cluster_data) for cluster_data in clusters.values())
        points_per_partition = total_points // num_reducers
        partition_id = 0
        clusters = sorted(clusters.items(), key=lambda x: x[0])

        for cluster_id, cluster_data in clusters.items():
            for point in cluster_data:
                val = str(cluster_id) + " " + str(point[1][0]) + " " + str(point[1][1])
                partitions[partition_id].append(val)
                partition_counts[partition_id] += 1

                if partition_counts[partition_id] >= points_per_partition:
                    partition_id = (partition_id + 1) % num_reducers
                
                if partition_id not in self.partition_dict:
                    self.partition_dict[partition_id] = []
                self.partition_dict[partition_id].append(val)

        for partition_id, partition_data in partitions.items():
            partition_file = os.path.join(map_dir, f"partition_{partition_id}.txt")
            with open(partition_file, "a+") as f:
                for value in partition_data:
                    f.write(f"{value}\n")

        return partitions

    def map(self, request, context):
        if p_map == 0:
            return worker.status(code=404, msg="FAIL")
        file = request.path
        num_clusters = request.numClusters
        num_reducers = request.numReducers
        centroids = request.centroids
        centroids = np.array(centroids).reshape((num_clusters, -1))
        
        print("[!] [MAPPER] K-means Map operation: file='%s', num_clusters=%d" % (file, num_clusters))

        data_points = []
        with open(file, "r") as f:
            for line in f:
                x, y = map(float, line.strip().split(','))
                data_points.append([x, y])

        clusters = {i: [] for i in range(num_clusters)}

        for point in data_points:
            distances = [np.linalg.norm(np.array(point) - np.array(c)) for c in centroids]
            nearest_centroid = np.argmin(distances)
            clusters[nearest_centroid].append([list(centroids[nearest_centroid]), point])

        self.partition(clusters, num_reducers, request.mapID)
        return worker.status(code=200, msg="OK")

    def shuffle_and_sort(self, file_id, num_mappers):
        print("Entering function")
        sorted_partitions = {}
        
        for i in num_mappers:
            port = 4001 + i
            channel = None
            try:
                print(f"Connecting to mapper on port: {port}")
                channel = grpc.insecure_channel(f'localhost:{port}')
                stub = worker_grpc.WorkerStub(channel)
                reducer_id = int(file_id)
                response = stub.sendPartitionedData(worker.PartitionRequest(reducerID=reducer_id))
                print(f"RPC Connection established with mapper {i} from reducer, processing data...")
                
                for line in response.data:
                    k, p1, p2 = line.strip().split(" ")
                    if k not in sorted_partitions:
                        sorted_partitions[k] = []
                    sorted_partitions[k].append([float(p1), float(p2)])
                    
                print(f"Data processed for mapper {i}")

            except grpc.RpcError as e:
                print(f"RPC error encountered with mapper {i} on port {port}: {e}")
            except Exception as e:
                print(f"Exception encountered with mapper {i}: {e}")
            finally:
                if channel:
                    channel.close()
                    print(f"Channel to mapper {i} on port {port} closed.")

        print("SUCCESS [MAPPER TO REDUCER] Shuffling and sorting complete.")
        return sorted_partitions

    def calculate_centroid(self, clusters):
        final_centroids = {}
        for key, points in clusters.items():
            num_points = len(points)
            if num_points == 0:
                return None
            centroid = [0] * len(points[0])
            for point in points:
                for i in range(len(point)):
                    centroid[i] += point[i]
            centroid = [x / num_points for x in centroid]
            final_centroids[key] = centroid
        print("[!] [REDUCER] New centroids: ", final_centroids)
        return final_centroids

    def reduce(self, request, context):
        if p_red == 0:
            return worker.status(code=404, msg="FAIL")
        print("[!] [REDUCER] K-means Reduce operation")
        rid = request.id
        numMaps = request.mapIDs

        print("[!] [REDUCER] Shuffling and Sorting...")
        sorted_part = self.shuffle_and_sort(rid, numMaps)
        final_centroids = self.calculate_centroid(sorted_part)

        reduce_dir = f"reduce_output_{rid}"
        os.makedirs(reduce_dir, exist_ok=True)

        for key, new_centroid in final_centroids.items():
            output_file = os.path.join(reduce_dir, f"output_{key}.txt")
            with open(output_file, "a+") as f:
                f.write(f"{key} {new_centroid}\n")
        return worker.status(code=200, msg=str(final_centroids))

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    worker_grpc.add_WorkerServicer_to_server(Worker(), server)
    port = sys.argv[1]
    # if port == '4002':
    #     global p_map
    #     p_map = 0
    server.add_insecure_port("127.0.0.1:%s" % (port))
    server.start()
    print("Worker running on 127.0.0.1:%s" % (port))
    try:
        print("Worker is on | nbr threads %i" % (threading.active_count()))
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        server.stop(0)

if __name__ == "__main__":
    server()
