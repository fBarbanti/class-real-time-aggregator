import csv
import os,logging

from scipy.spatial import cKDTree as KDTree

# Class that defines the reverse geocoder 
class GeocodeData:
    def __init__(self, geocode_filename):
        self.coordinates, self.locations = self.read(geocode_filename)
        self.tree = KDTree(self.coordinates)

    def read(self, geocode_filename):
        coordinates, locations = [], []
        if os.path.exists(geocode_filename):
            rows = csv.reader(open(geocode_filename))
        else:
            print("File not found")
            return
        for full_id,highway,name,lanes,bridge,lit,unique_id,x,y in rows:
            # Add the latitude, longitude tuple as a coordinate 
            coordinates.append((y, x))
            # Enter street, house number, neighborhood and id in a dictionary, then add the dictionary 
            locations.append(dict(name=name, highway=highway, lanes=lanes, bridge = bridge, lit=lit, id=full_id, unique_id=unique_id))
        return coordinates, locations

    def query(self, coordinates):
        # Find closest match to this list of coordinates
        try:
            # k = number of neighbors to return
            distances, indices = self.tree.query(coordinates, k=1)
        except ValueError as e:
            logging.info('Unable to parse coordinates: {}'.format(coordinates))
            raise e
        else:
            if(indices == len(self.locations)):
                results = dict(name="null", highway="null", lanes="null", bridge = "null", lit="null", id="null", unique_id="null").values()
            else:
                results = self.locations[indices].values()
            return results
