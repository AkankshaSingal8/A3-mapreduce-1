import os
import sys
import grpc
import glob
import time
from concurrent import futures
import numpy as np
import threading
import pyautogui
from collections import defaultdict

import driver_pb2 as driver
import driver_pb2_grpc as driver_grpc
import worker_pb2 as worker
import worker_pb2_grpc as worker_grpc

class Driver(driver_grpc.DriverServicer):
    def __init__(self):
        super().__init__()
        self.mappers = {}
        self.reducers = {}
        self.iterations = 0
        self.centroids = []
        self.treatFiles = dict()
        self.updates = []
        self.nextMapper = 0
        self.nextReducer = 0
    
    def launchDriver(self, request, context):
        def initialize_centroids(num_clusters):
            with open("./pts.txt", 'r') as file:
                lines = file.readlines()
            pts = []
            for line in lines:
                pt = [float(coord) for coord in line.strip().split(',')]
                pts.append(pt)
            pts = np.array(pts)
            ind = np.random.choice(np.arange(len(pts)), size = num_clusters)
            self.centroids = pts[ind]
            # with open("./points/p1.txt", 'r') as file:
            #     lines = file.readlines()
            # for line in lines[:num_clusters]:
            #     centroid = [float(coord) for coord in line.strip().split(',')]
            #     self.centroids.append(centroid)
            # self.centroids = np.array(self.centroids)
            self.centroids = np.array(sorted(np.array(self.centroids).tolist()))
            self.centroids = np.array([[-2000, -2000], [-500,-500], [0, 0], [500, 500], [2000, 2000]], dtype=np.float32)
            print("Initial Centroids:")
            print(self.centroids)

        def map_kmeans(key, file, mid, num_clusters, num_reds):
            print(f"[*] [DRIVER] [MAP] Map operation '{mid}' on the file '{file}' is sent to worker '{key}'...")
            try:
                self.mappers[key][0] = 1
                centroids_list = self.centroids.flatten().astype(float).tolist()
                r = worker.kmeansInput(path=file, mapID=int((str(key))[-1]) - 1, numClusters=num_clusters, centroids=centroids_list, numReducers=num_reds)
                r = self.mappers[key][1].map(r)
                if r.code != 200:
                    print(f"[-] [DRIVER] [MAP] Map operation '{mid}' failed on worker '{key}'. Retrying with another mapper...")
                    new_mapper = get_mapper()
                    if new_mapper:
                        print(f"[*] [DRIVER] [MAP] Retrying map operation '{mid}' on worker '{new_mapper}'...")
                        return map_kmeans(new_mapper, file, mid, num_clusters, num_reds)
                    else:
                        print(f"[-] [DRIVER] [MAP] No available mapper to retry map operation '{mid}'.")
                else:
                    print(f"[!] [DRIVER] [MAP] Map operation '{mid}' on the file '{file}' terminated with code: '{r.code}' and message: {r.msg}.")
            except Exception as e:
                print(f"[-] [DRIVER] [MAP] Error occurred while sending map operation '{mid}' to worker '{key}': {str(e)}")
                print(f"[-] [DRIVER] [MAP] Retrying map operation '{mid}' on another mapper...")
                new_mapper = get_mapper()
                if new_mapper:
                    print(f"[*] [DRIVER] [MAP] Retrying map operation '{mid}' on worker '{new_mapper}'...")
                    return map_kmeans(new_mapper, file, mid, num_clusters, num_reds)
                else:
                    print(f"[-] [DRIVER] [MAP] No available mapper to retry map operation '{mid}'.")
            self.mappers[key][0] = 0
            return r

        def reduce_kmeans(key, rid, num_files):
            print(f"[*] [DRIVER] [REDUCE] Reduce operation '{rid}' is sent to worker '{key}'...")
            try:
                self.reducers[key][0] = 1
                r = worker.kmeansReduce(id=int((str(key))[-1]) - 1 - mappers, mapIDs=num_files)
                r = self.reducers[key][1].reduce(r)
                if r.code != 200:
                    print(f"[-] [DRIVER] [MAP] Reduce operation '{rid}' failed on worker '{key}'. Retrying with another reducer...")
                    new_reducer = get_reducer()
                    if new_reducer:
                        print(f"[*] [DRIVER] [MAP] Retrying reduce operation '{rid}' on worker '{new_reducer}'...")
                        return reduce_kmeans(new_reducer, rid, num_files)
                    else:
                        print(f"[-] [DRIVER] [MAP] No available reducer to retry reduce operation '{rid}'.")
                else:
                    print(f"[!] [DRIVER] [REDUCE] Reduce operation '{rid}' terminated with code: '{r.code}' and centroids: {r.msg}.")
            except Exception as e:
                print(f"[-] [DRIVER] [REDUCE] Error occurred while sending reduce operation '{rid}' to worker '{key}': {str(e)}")
                print(f"[-] [DRIVER] [REDUCE] Retrying reduce operation '{rid}' on another reducer...")
                new_reducer = get_reducer()
                if new_reducer:
                    print(f"[*] [DRIVER] [REDUCE] Retrying reduce operation '{rid}' on worker '{new_reducer}'...")
                    return reduce_kmeans(new_reducer, rid, num_files)
                else:
                    print(f"[-] [DRIVER] [REDUCE] No available reducer to retry reduce operation '{rid}'.")
            self.reducers[key][0] = 0
            temp = list(eval(r.msg).values())
            # print(list(eval(r.msg).values()))
            for i in temp:
                self.updates.append(i)
            return r

        def get_mapper():
            for k, v in self.mappers.items():
                if v[0] == 0:
                    return k
            return False
        
        def get_active_mapper():
            active_mapper = []
            for k, v in self.mappers.items():
                if v[0] == 0:
                    active_mapper.append(int((str(k))[-1]) - 1)
            return active_mapper
        
        def get_reducer():
            for k, v in self.reducers.items():
                if v[0] == 0:
                    return k
            return False

        def delete_files(directory):
            files = glob.glob(directory + "/*.txt")
            for f in files:
                os.remove(f)
        
        print("[!] [DRIVER] [CONFIG] Driver is launching")
        files = glob.glob(request.dirPath + "/*.txt")
        reducers = request.numReducers
        mappers = request.numMappers
        num_clusters = request.numClusters
        self.iterations = request.iters

        ports = [int(port) for port in request.ports.split('|')]
        print("[!] [DRIVER] [CONFIG] Requested %i workers with the following ports: [%s]." % (len(ports), ' '.join(str(s) for s in ports)))
        
        for file in files:
            self.treatFiles[file] = 0

        def connect_to_worker(port, is_mapper):
            worker_type = "mapper" if is_mapper else "reducer"
            try:
                channel = grpc.insecure_channel(f'localhost:{port}')
                grpc.channel_ready_future(channel).result(timeout=10)
            except grpc.FutureTimeoutError:
                print(f"[-] [ERROR] Could not connect to {worker_type} '{port}'.")
                return None
            else:
                print(f"[!] [DRIVER] [CONFIG] Connection with {worker_type} '{port}' established.")
                return channel
        
        for port in ports[:mappers]:
            pyautogui.hotkey('ctrl', 'shift', '~')
            time.sleep(1)
            pyautogui.typewrite(f"python worker.py {port}" + '\n')
            channel = connect_to_worker(port, is_mapper=True)
            if channel:
                self.mappers[port] = [0, worker_grpc.WorkerStub(channel)]
                self.mappers[port][1].setDriverPort(worker.driverPort(port=int(sys.argv[1])))

        for port in ports[mappers:]:
            pyautogui.hotkey('ctrl', 'shift', '~')
            time.sleep(1)
            pyautogui.typewrite(f"python worker.py {port}" + '\n')
            channel = connect_to_worker(port, is_mapper=False)
            if channel:
                self.reducers[port] = [0, worker_grpc.WorkerStub(channel)]
                self.reducers[port][1].setDriverPort(worker.driverPort(port=int(sys.argv[1])))

        print("[!] [DRIVER] [CONFIG]  Registered: %i map operations and %i reduce operations" % (mappers, reducers))
        print("[!] [DRIVER] [KMEANS] Initializing centroids...")
        initialize_centroids(num_clusters)

        for i in range(self.iterations):
            print(f"[!] [DRIVER] [KMEANS] Starting iteration {i+1}/{self.iterations}")
            start_time = time.time()
            
            if i != 0:
                self.centroids = np.array(sorted(self.centroids.tolist()))
                temp_centroids = np.array(sorted(np.array(self.updates).tolist()))
                print(self.centroids)
                print(self.updates)
                diff = np.linalg.norm((self.centroids - temp_centroids))

                if diff <= 1.:
                    print(f"[!] [DRIVER] [KMEANS] Centroids converged. Terminating the algorithm.")
                    break
                
                self.centroids = temp_centroids
                self.updates = []
                print(f"[!] [DRIVER] [KMEANS] Updated centroids: {self.centroids}")
                with open("centroids.txt", "w") as f:
                    f.write(str(self.centroids))
            
            if i != self.iterations - 1:
                delete_files("./map_output_*")
                delete_files("./reduce_output_*")

            # Dispatch map tasks
            with futures.ThreadPoolExecutor() as executor:
                for idx, file in enumerate(files):
                    tmp_worker = 4001 + self.nextMapper % mappers
                    self.nextMapper += 1
                    while tmp_worker is False:
                        print(f"[!] [DRIVER] [KMEANS] Map operation '{idx}' is paused due to all workers being occupied.")
                        # time.sleep(5)
                        tmp_worker = get_mapper()
                    print(f"[!] [DRIVER] [KMEANS] Launching map operation '{idx}' on the file '{file}' started.")
                    executor.submit(map_kmeans, key=tmp_worker, file=file, mid=idx, num_clusters=num_clusters, num_reds=reducers)
            
            executor.shutdown(wait=True)
            print(f"[!] [DRIVER] [KMEANS] Map phase terminated in '{time.time() - start_time}' second(s).")

            start_time = time.time()
            # Dispatch reduce tasks
            with futures.ThreadPoolExecutor() as executor:
                for idx in range(reducers):
                    print(f"[*] [DRIVER] [KMEANS] Finding a worker for reduce operation '{idx}'...")
                    tmp_worker = 4001 + mappers + (self.nextReducer % reducers)
                    self.nextReducer += 1
                    while tmp_worker is False:
                        print(f"[!] [DRIVER] [KMEANS] Reduce operation '{idx}' is paused due to all workers being occupied.")
                        # time.sleep(5)
                        tmp_worker = get_reducer()
                    executor.submit(reduce_kmeans, key=tmp_worker, rid=idx, num_files=get_active_mapper())
            
            executor.shutdown(wait=True)
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
