import sys
import grpc

import driver_pb2 as driver
import driver_pb2_grpc as driver_grpc

def run():
    channel = grpc.insecure_channel('localhost:4000')
    try:
        print("[*] Connecting to the server...")
        grpc.channel_ready_future(channel).result(timeout=10)
    except grpc.FutureTimeoutError:
        sys.exit('[-] [ERROR] Could not connect to the server.')
    else:
        print("[!] Connection established.")
        arg = sys.argv
        
        stub = driver_grpc.DriverStub(channel)
        ports = '|'.join(arg[5:])
        req = driver.launchData(dirPath=arg[1], m=int(arg[2]), ports=ports, numClusters=int(arg[3]), dimension=int(arg[4]))
        response = stub.launchDriver(req)
        print("[!] Operation terminated with code: %i and message: %s"%(response.code, response.msg))

if __name__ == "__main__":
    run()
