import sys
import grpc
import glob
import time
from concurrent import futures
import numpy as np
import threading
import pyautogui

import driver_pb2 as driver
import driver_pb2_grpc as driver_grpc
import worker_pb2 as worker
import worker_pb2_grpc as worker_grpc

class Driver(driver_grpc.DriverServicer):
    def __init__(self):
        super().__init__()
        self.mappers = {}
        self.reducers = {}
        self.iterations = 5
        self.centroids = []
        self.treatFiles = dict()
        self.updates = []
    
    def launchDriver(self, request, context):
        def initialize_centroids(num_clusters, dimension):
            with open("./points/p1.txt", 'r') as file:
                lines = file.readlines()
            for line in lines[:num_clusters]:
                centroid = [float(coord) for coord in line.strip().split(',')]
                self.centroids.append(centroid)
            self.centroids = np.array(self.centroids)            
            print("Initial Centroids:")
            print(self.centroids)

        def map_kmeans(key, file, mid, num_clusters, num_reds):
            print(f"[*] [DRIVER] [MAP] Map operation '{mid}' on the file '{file}' is sent to worker '{key}'...")
            self.mappers[key][0] = 1
            centroids_list = self.centroids.flatten().astype(float).tolist()
            r = worker.kmeansInput(path=file, mapID=mid, numClusters=num_clusters, centroids=centroids_list, numReducers=num_reds)
            r = self.mappers[key][1].map(r)
            self.mappers[key][0] = 0
            print(f"[!] [DRIVER] [MAP] Map operation '{mid}' on the file '{file}' terminated with code: '{r.code}' and message: {r.msg}.")
            return r

        def reduce_kmeans(key, rid, num_files):
            print(f"[*] [DRIVER] [REDUCE] Reduce operation '{rid}' is sent to worker '{key}'...")
            self.reducers[key][0] = 1
            r = worker.kmeansReduce(id=rid, mapNums=num_files)
            r = self.reducers[key][1].reduce(r)
            self.reducers[key][0] = 0
            self.updates.append(eval(r.msg))
            print(f"[!] [DRIVER] [REDUCE] Reduce operation '{rid}' terminated with code: '{r.code}' and centroids: {r.msg}.")
            return r

        def get_mapper():
            for k, v in self.mappers.items():
                if v[0] == 0:
                    return k
            return False
        
        def get_reducer():
            for k, v in self.reducers.items():
                if v[0] == 0:
                    return k
            return False
        
        print("[!] [DRIVER] [CONFIG] Driver is launching")
        files = glob.glob(request.dirPath + "/*.txt")
        reducers = request.m
        num_clusters = request.numClusters
        dimension = request.dimension

        ports = [int(port) for port in request.ports.split('|')]
        print("[!] [DRIVER] [CONFIG] Requested %i workers with the following ports: [%s]." % (len(ports), ' '.join(str(s) for s in ports)))
        
        for file in files:
            self.treatFiles[file] = 0

        for port in ports[:len(files)]:
            pyautogui.hotkey('ctrl', 'shift', '~')
            pyautogui.typewrite(f"python worker.py {port}" + '\n')
            channel = grpc.insecure_channel(f'localhost:{port}')
            print(f"[*] [DRIVER] [CONFIG] Connecting to mapper with port: {port}...")
            try:
                grpc.channel_ready_future(channel).result(timeout=10)
            except grpc.FutureTimeoutError:
                sys.exit(f"[-] [ERROR] Could not connect to mapper '{port}'.")
            self.mappers[port] = [0, worker_grpc.WorkerStub(channel)]
            self.mappers[port][1].setDriverPort(worker.driverPort(port=int(sys.argv[1])))
            print(f"[!] [DRIVER] [CONFIG] Connection with mapper '{port}' established.")
        
        for port in ports[len(files):]:
            pyautogui.hotkey('ctrl', 'shift', '~')
            pyautogui.typewrite(f"python worker.py {port}" + '\n')
            channel = grpc.insecure_channel(f'localhost:{port}')
            print(f"[*] [DRIVER] [CONFIG] Connecting to reducer with port: {port}...")
            try:
                grpc.channel_ready_future(channel).result(timeout=10)
            except grpc.FutureTimeoutError:
                sys.exit(f"[-] [ERROR] Could not connect to reducer '{port}'.")
            self.reducers[port] = [0, worker_grpc.WorkerStub(channel)]
            self.reducers[port][1].setDriverPort(worker.driverPort(port=int(sys.argv[1])))
            print(f"[!] [DRIVER] [CONFIG] Connection with reducer '{port}' established.")


        print("[!] [DRIVER] [CONFIG]  Registered: %i map operations and %i reduce operations" % (len(files), reducers))
        print("[!] [DRIVER] [KMEANS] Initializing centroids...")
        initialize_centroids(num_clusters, dimension)

        for i in range(self.iterations):
            print(f"[!] [DRIVER] [KMEANS] Starting iteration {i+1}/{self.iterations}")
            start_time = time.time()
            if i != 0:
                self.centroids = []
                for cluster in range(num_clusters):
                    x = 0
                    y = 0
                    for reducer in range(reducers):
                        x += self.updates[reducer][cluster][0]
                        y += self.updates[reducer][cluster][1]
                    x /= reducers
                    y /= reducers
                    self.centroids.append([x, y])
                self.updates = []
                self.centroids = np.array(self.centroids)
                print(f"[!] [DRIVER] [KMEANS] Updated centroids: {self.centroids}")
                with open("centroids.txt", "w") as f:
                    f.write(str(self.centroids))
            
            # Dispatch map tasks
            with futures.ThreadPoolExecutor() as executor:
                for idx, file in enumerate(files):
                    print(f"[*] [DRIVER] [KMEANS] Launching map operation '{idx}' on the file '{file}'...")
                    tmp_worker = get_mapper()
                    while tmp_worker is False:
                        print(f"[!] [DRIVER] [KMEANS] Map operation '{idx}' is paused due to all workers being occupied.")
                        time.sleep(5)
                        tmp_worker = get_mapper()
                    print(f"[!] [DRIVER] [KMEANS] Launching map operation '{idx}' on the file '{file}' started.")
                    executor.submit(map_kmeans, key=tmp_worker, file=file, mid=idx, num_clusters=num_clusters, num_reds=reducers)
            
            print(f"[!] [DRIVER] [KMEANS] Map phase terminated in '{time.time() - start_time}' second(s).")

            start_time = time.time()
            # Dispatch reduce tasks
            with futures.ThreadPoolExecutor() as executor:
                for idx in range(reducers):
                    print(f"[*] [DRIVER] [KMEANS] Finding a worker for reduce operation '{idx}'...")
                    tmp_worker = get_reducer()
                    while tmp_worker is False:
                        print(f"[!] [DRIVER] [KMEANS] Reduce operation '{idx}' is paused due to all workers being occupied.")
                        time.sleep(5)
                        tmp_worker = get_reducer()
                    executor.submit(reduce_kmeans, key=tmp_worker, rid=idx, num_files=len(files))
            
            print(f"[!] [DRIVER] [KMEANS] Reduce phase terminated in '{time.time() - start_time}' second(s).")
        
        for port, (stat, stub) in self.mappers.items():
            stub.die(worker.empty())
        
        for port, (stat, stub) in self.reducers.items():
            stub.die(worker.empty())
    
        return driver.status(code=200, msg="OK")


def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    driver_grpc.add_DriverServicer_to_server(Driver(), server)
    port = sys.argv[1]
    server.add_insecure_port("127.0.0.1:%s" % (port))
    server.start()
    print("Driver running on 127.0.0.1:%s" % (port))
    try:
        print("Driver is on | nbr threads %i" % (threading.active_count()))
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        server.stop(0)

if __name__ == "__main__":
    server()
