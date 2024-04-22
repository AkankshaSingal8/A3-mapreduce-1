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

    def setDriverPort(self, request, context):
        print("Old driver port", self.driver_port)
        self.driver_port = request.port
        print("New driver port", self.driver_port)
        return worker.status(code=200, msg="OK")

    def die(self, request, context):
        return worker.empty()

    def partition(self, clusters, num_reducers, map_id):
        print("[!] [MAPPER] Partitioning clusters...")
        map_dir = f"map_output_{map_id}"
        os.makedirs(map_dir, exist_ok=True)
        
        partitions = {i: [] for i in range(num_reducers)}
        partition_counts = {i: 0 for i in range(num_reducers)}

        total_points = sum(len(cluster_data) for cluster_data in clusters.values())
        points_per_partition = total_points // num_reducers
        partition_id = 0

        for cluster_id, cluster_data in clusters.items():
            for point in cluster_data:
                val = str(cluster_id) + " " + str(point[1][0]) + " " + str(point[1][1])
                partitions[partition_id].append(val)
                partition_counts[partition_id] += 1

                if partition_counts[partition_id] >= points_per_partition:
                    partition_id = (partition_id + 1) % num_reducers

        for partition_id, partition_data in partitions.items():
            partition_file = os.path.join(map_dir, f"partition_{partition_id}.txt")
            with open(partition_file, "w") as f:
                for value in partition_data:
                    f.write(f"{value}\n")

        return partitions

    def map(self, request, context):
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
        if p_map == 0:
            return worker.status(code=404, msg="FAIL")
        return worker.status(code=200, msg="OK")

    def shuffle_and_sort(self, file_id, num_mappers):
        sorted_partitions = {}

        for i in range(num_mappers):
            partition_file = os.path.join(f"map_output_{i}", f"partition_{file_id}.txt")
            with open(partition_file, "r") as f:
                for line in f:
                    k, p1, p2 = line.strip().split(" ")
                    if k not in sorted_partitions:
                        sorted_partitions[k] = []
                    sorted_partitions[k].append([float(p1), float(p2)])

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
        print("[!] [REDUCER] K-means Reduce operation")
        rid = request.id
        numMaps = request.mapNums

        print("[!] [REDUCER] Shuffling and Sorting...")
        sorted_part = self.shuffle_and_sort(rid, numMaps)
        final_centroids = self.calculate_centroid(sorted_part)

        reduce_dir = f"reduce_output_{rid}"
        os.makedirs(reduce_dir, exist_ok=True)

        for key, new_centroid in final_centroids.items():
            output_file = os.path.join(reduce_dir, f"output_{key}.txt")
            with open(output_file, "w") as f:
                f.write(f"{key} {new_centroid}\n")
        if p_red == 0:
            return worker.status(code=404, msg="FAIL")
        return worker.status(code=200, msg=str(final_centroids))

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    worker_grpc.add_WorkerServicer_to_server(Worker(), server)
    port = sys.argv[1]
    if port == '4001':
        global p_red
        p_red = 0
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
