# Author: Beyza Tugce Bilgic

""" 
Offloading scheduler to control offloading decisions for vehicular applications 
accoding to resource usage and network congestion of accessible (nearest) edge servers
"""
import json
import random
import requests
import logging
import sys
from time import time
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("debug.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

class OffloadingScheduler:

    host_ips = {
        "edge1": "138.246.237.7",
        "edge2": "138.246.236.237",
        "edge3": "138.246.237.5"
    }

    app_types = {
        "mobilenet": "https://serverless-mobilenet-6ag2jbmdqa-uc.a.run.app",
        "squeezenet": "https://serverless-squeezenet-6ag2jbmdqa-uc.a.run.app",
        "shufflenet": "https://serverless-shufflenet-6ag2jbmdqa-uc.a.run.app",
        "binaryalert": "https://serverless-binaryalert-6ag2jbmdqa-uc.a.run.app"
    }

    time_limit = 15
    proxy_port = 8280
    metric_port = 8180
    counter = 1

    def __init__(self, vehicle, carla_world=None, base_station_roles=None) -> None:
        
        self.vehicle = vehicle
        self.base_station_roles = base_station_roles
        self.carla_world = carla_world if carla_world is not None \
            else self.vehicle.get_world()

        self.ego_pos = None

    def post_url(self, args):
        return requests.post(args[0], data=args[1])

    def offload_to_optimal(self, ego_pos):
        """
        Offload to the optimal base station having minimum reponse time.
        """
        self.ego_pos = ego_pos
        bs_coverage_thresh = 120
        list_of_urls = []
        #app = self.app_types[random.randint(1,4)]
        #print(f"Vehicle location: {ego_pos.location}")
        for app in self.app_types.keys():
            nearest_bs = self.find_nearest_base_station()
            if nearest_bs and nearest_bs[1] < bs_coverage_thresh:
                # find ip address of the nearest base station
                bs_hostname = self.base_station_roles[nearest_bs[0]]
                bs_ip = self.host_ips[bs_hostname]

                # find optimal base station to offload request according to min response time
                edge_metrics = self.get_metrics(app, nearest_bs)
                optimal_edge = sorted(edge_metrics.items(), key=lambda m: m[1])[0][0]
    
                if edge_metrics[optimal_edge] > self.time_limit:
                    req = json.dumps({"request_start": time()})
                    requests.post(f"{self.app_types[app]}/init", req)
                    requests.post(f"{self.app_types[app]}/run")
                    #list_of_urls.append((f"{self.app_types[app]}/init", req))
                    print(f"Vehicle {self.vehicle.id} -> Offload {app} to the remote cloud")
                else:
                    req = json.dumps({"node": optimal_edge, "app": app, "request_start": time()})
                    list_of_urls.append((f"http://{bs_ip}:{self.proxy_port}/proxy", req))
                    print(f"Vehicle {self.vehicle.id} connects to {bs_hostname} -> Offload {app} to optimal {optimal_edge} from {edge_metrics}")
            else:
                print(f"Vehicle {self.vehicle.id} -> Offload {app} to the remote cloud")

        for _ in range(self.counter):
            with ThreadPoolExecutor(max_workers=4) as pool:
                response_list = list(pool.map(self.post_url, list_of_urls))
        self.counter += 1

    def offload_to_nearest(self, ego_pos):
        """
        Offload to the nearest base station that the vehicle is under the coverage area of.
        """
        self.ego_pos = ego_pos
        bs_coverage_thresh = 120
        list_of_urls = []
        #app = self.app_types[random.randint(1,4)]

        for app in self.app_types:
            nearest_bs = self.find_nearest_base_station()
            if nearest_bs and nearest_bs[1] < bs_coverage_thresh:
                # find ip address of the nearest base station
                bs_hostname = self.base_station_roles[nearest_bs[0]]
                bs_ip = self.host_ips[bs_hostname]
                edge_metrics = self.get_metrics(app, nearest_bs)

                #if edge_metrics[bs_hostname] > self.time_limit:
                #    req = json.dumps({"request_start": time()})
                #    list_of_urls.append((self.app_types[app], req))
                #    print(f"Vehicle {self.vehicle.id} -> Offload {app} to the remote cloud")
                #else:
                req = json.dumps({"node": bs_hostname, "app": app, "request_start": time()})
                list_of_urls.append((f"http://{bs_ip}:{self.proxy_port}/proxy", req))
                print(f"Vehicle {self.vehicle.id} connects to {bs_hostname} -> Offload {app} to nearest {bs_hostname}")
            else:
                print(f"Vehicle {self.vehicle.id} -> Offload {app} to the remote cloud!")

        for _ in range(self.counter):
            with ThreadPoolExecutor(max_workers=4) as pool:
                response_list = list(pool.map(self.post_url, list_of_urls))
        self.counter += 1

    def get_metrics(self, app: str, nearest_bs: tuple) -> dict:
        """
        Collect metrics from each node for the app type to be offloaded
        """
        bs_hostname = self.base_station_roles[nearest_bs[0]]
        bs_ip = self.host_ips[bs_hostname]
        req_list = [json.dumps({"node": edge, "app": app}) for edge in self.host_ips.keys()]
        edge_metrics = {}
        for req in req_list:
            resp = requests.post(f"http://{bs_ip}:{self.metric_port}/metrics", data=req)
            if resp.status_code != 200:
                continue
            metrics = json.loads(resp.text)
            pod_num = metrics["pod_number"]
            if pod_num == 0:
                continue
            pod_instances = metrics["pod_instances"]
            res_total = 0.0
            for _, pod_metric in pod_instances.items():
                res_total += pod_metric["p50_res_time"]
            edge_metrics[json.loads(req)["node"]] = res_total / pod_num
        return edge_metrics

    def find_nearest_base_station(self):
        """
        Find the nearest one.

        Returns
        -------
        base_station_id : int
            The id of nearest base station to potentially offload tasks.
        """
        sorted_bs_list = self.sort_base_stations()
        nearest = sorted_bs_list[0] if sorted_bs_list else None
        return nearest

    def sort_base_stations(self):
        """
        Sort the base stations according to their distances to the vehicle.

        Returns
        -------
        bs_sorted : list
            The list of base stations sorted by their distance to the vehicle.
        """
        world = self.carla_world
        base_station_list = world.get_actors().filter("static.prop.box01")
        bs_dict = dict()
        for bs in base_station_list:
            bs_dict[bs.id] = self.dist(bs)
        bs_sorted = sorted(bs_dict.items(), key = lambda kv: (kv[1], kv[0]))
        #print("Base stations: ",bs_sorted)
        return bs_sorted 

    def dist(self, a):
        """
        A fast method to retrieve the obstacle distance the ego
        vehicle from the server directly.

        Parameters
        ----------
        a : carla.actor
            The obstacle vehicle.

        Returns
        -------
        distance : float
            The distance between ego and the target actor.
        """
        return a.get_location().distance(self.ego_pos.location)    

