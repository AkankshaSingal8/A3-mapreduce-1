import sys
import grpc

import driver_pb2 as driver
import driver_pb2_grpc as driver_grpc
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--dir_path', type = str)
parser.add_argument('-m', '--mappers', type = int)
parser.add_argument('-r', '--reducers', type = int)
parser.add_argument('-k', '--centroids', type = int)
parser.add_argument('-i', '--iterations', type = int)

import socket
from contextlib import closing
def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]
        
def run(dir_path, n_mappers = 5, n_reducers = 5, n_centroids = 3, n_iters = 50):
    channel = grpc.insecure_channel('localhost:4000')
    try:
        print("[*] Connecting to the server...")
        grpc.channel_ready_future(channel).result(timeout=10)
    except grpc.FutureTimeoutError:
        sys.exit('[-] [ERROR] Could not connect to the server.')
    else:
        print("[!] Connection established.")
        stub = driver_grpc.DriverStub(channel)

        ports = " ".join([str(find_free_port()) for _ in range(n_reducers + n_mappers)])
        print(f'[*] Ports are: {ports}')
        req = driver.launchData(dirPath = dirpath,
                                m = n_mappers,
                                n = n_reducers,
                                ports=ports,
                                numClusters=n_centroids,
                                iterations=n_iterations)
        response = stub.launchDriver(req)
        print("[!] Operation terminated with code: %i and message: %s"%(response.code, response.msg))

if __name__ == "__main__":
    args = parser.parse_args()
    dirpath = args.dir_path
    n_mappers = args.mappers
    n_reducers = args.reducers
    n_centroids = args.centroids
    n_iterations = args.iterations
    
    run(dir_path = dirpath,
        n_mappers = n_mappers,
        n_reducers = n_reducers,
        n_centroids = n_centroids,
        n_iters = n_iterations)
