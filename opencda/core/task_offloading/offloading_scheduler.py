# Author: Beyza Tugce Bilgic

""" 
Offloading scheduler to control offloading decisions for vehicular applications 
accoding to resource usage and network congestion of accessible (nearest) edge servers
"""
class OffloadingScheduler:

    def __init__(self, vehicle, carla_world=None, base_station_roles=None) -> None:
        
        self.vehicle = vehicle
        self.base_station_roles = base_station_roles
        self.carla_world = carla_world if carla_world is not None \
            else self.vehicle.get_world()

        self.ego_pos = None

    def offload_to_nearest(self, ego_pos):
        """
        Offload to the nearest base station that the vehicle is under the coverage area of.
        """
        self.ego_pos = ego_pos
        bs_coverage_thresh = 50
     
        print(f"Vehicle location: {ego_pos.location}")
        sorted_bs_list = self.sort_base_stations()

        if sorted_bs_list:
            nearest_bs = self.find_nearest_base_station(sorted_bs_list)
            
            if nearest_bs[1] < bs_coverage_thresh:
                nearest_bs_role = self.base_station_roles[nearest_bs[0]]
                print(f"Vehicle {self.vehicle.id} -> Offload to bs {nearest_bs_role}")
            else:
                print(f"Vehicle {self.vehicle.id} -> Local execution!")


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

    def find_nearest_base_station(self, sorted_bs_list: list):
        """
        Find the nearest one.

        Returns
        -------
        base_station_id : int
            The id of nearest base station to potentially offload tasks.
        """
        nearest = sorted_bs_list[0] if sorted_bs_list else None
        return nearest

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

        


