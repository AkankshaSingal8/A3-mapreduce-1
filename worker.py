import os
import sys
import grpc
import glob
import time
from collections import defaultdict
import numpy as np
from concurrent import futures
import threading

import worker_pb2 as worker
import worker_pb2_grpc as worker_grpc

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

    def map(self, request, context):
        file = request.path
        num_clusters = request.numClusters
        centroids = request.centroids
        
        print("[!] [WORKER] K-means Map operation: file='%s', num_clusters=%d" % (file, num_clusters))

        # Create a directory for the current map operation
        map_dir = f"map_output_{request.mapID}"
        os.makedirs(map_dir, exist_ok=True)

        # Read data points from file
        with open(file, "r") as f:
            lines = f.readlines()
            data_points = [list(map(float, line.strip().split())) for line in lines]

        # Find nearest centroid for each data point
        results = defaultdict(list)
        for point in data_points:
            min_distance = sys.maxsize
            nearest_centroid = None
            for i, centroid in enumerate(centroids):
                distance = np.linalg.norm(np.array(point) - centroid)
                if distance < min_distance:
                    min_distance = distance
                    nearest_centroid = i
            results[nearest_centroid].append(point)

        # Write results into a text file within the directory
        for centroid, points in results.items():
            output_file = os.path.join(map_dir, f"output_{centroid}.txt")
            with open(output_file, "w") as f:
                for point in points:
                    f.write(" ".join(str(coord) for coord in point) + "\n")

        # # Emit intermediate key-value pairs
        # for centroid, points in results.items():
        #     yield worker.kmeansIntermediate(centroid=centroid, points=points)
        
        return worker.status(code=200, msg="OK")

    def reduce(self, request_iterator, context):
        centroid = request_iterator[0].centroid
        points = [point for item in request_iterator for point in item.points]
        
        print("[!] [WORKER] K-means Reduce operation: centroid=%d" % centroid)

        # Compute new centroid
        if points:
            new_centroid = np.mean(points, axis=0)
        else:
            new_centroid = np.zeros(len(points[0]))  # Initialize to zero if no points

        # Create a directory for the current reduce operation
        reduce_dir = f"reduce_output_{centroid}"
        os.makedirs(reduce_dir, exist_ok=True)

        # Write results into a text file within the directory
        output_file = os.path.join(reduce_dir, f"output_{centroid}.txt")
        with open(output_file, "w") as f:
            for point in new_centroid:
                f.write(str(point) + "\n")

        # Emit new centroid
        # yield worker.kmeansIntermediate(centroid=centroid, points=[list(new_centroid)])
        return worker.status(code=200, msg="OK")

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    worker_grpc.add_WorkerServicer_to_server(Worker(), server)
    port = sys.argv[1]
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
