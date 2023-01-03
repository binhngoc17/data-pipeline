"""
This class implements one merging logics for data from sources; this merging logics will work
as this:
- For single value, we assume that the data sources are ranked according to how
trusted the data source is: paperflies > patagonia > acme
- For list value, we try to merge the list of data together
"""

class PriorityMerger:
    def __init__(self, priority: list):
        self.priority = priority

    def merge(self, hotel_id: str, data: dict):
        output = {
            'id': hotel_id,
            'amenities': {},
            'location': {},
            'images': {}
        }

        # merge scalar field
        self.merge_scalar_fields(['destination_id', 'name', 'description'], data, output)

        # merge location field
        locations = {}
        for key in data:
            locations[key] = data[key].get('location')
        self.merge_scalar_fields(['lat', 'lng', 'address', 'city', 'country'], locations, output['location'])

        # merge list field
        self.merge_list_fields(['booking_conditions'], data, output)

        # merge amenities
        amenities = {}
        for key in data:
            amenities[key] = data[key].get('amenities')
        self.merge_list_fields(['room', 'general'], amenities, output['amenities'])

        # merge images
        images = {}
        for key in data:
            images[key] = data[key].get('images')
        self.merge_list_fields_with_key(['rooms', 'site', 'amenities'], images, output['images'])

    def merge_scalar_fields(self, scalar_fields: list, data: dict, output: dict):
        for field in scalar_fields:
            for source in self.priority:
                if data.get(source):
                    source_data = data[source]
                    if source_data.get(field):
                        output[field] = source_data[field]
                        break

    def merge_list_fields(self, list_fields: list, data: dict, output: dict):
        for field in list_fields:
            output[field] = []
            for source in self.priority:
                if data.get(source):
                    output[field].extend(data[source])

    def merge_list_fields_with_key(self, list_fields: list, data: dict, output: dict, key: str):
        # cache all the key value we already added to the list
        known_keys = []
        for field in list_fields:
            output[field] = []
            for source in self.priority:
                if data.get(source) and data[source][key] not in known_keys:
                    output[field].extend(data[source])
                    known_keys.append(data[source][key])


v1_priority_merger = PriorityMerger(['paperflies', 'patagonia', 'acme'])