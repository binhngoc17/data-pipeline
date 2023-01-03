"""
This class implements one merging logics for data from sources; this merging logics will work
as this:
- For single value, we assume that the data sources are ranked according to how
trusted the data source is: paperflies > patagonia > acme
- For list value, we try to merge the list of data together
"""

class PriorityMerger:

    # priority is the list of source name, the first item is highest priority while the
    # last item is of lowest priority
    def __init__(self, priority: list):
        self.priority = priority

    #data will be in format: { source_name: transformed_data_from_source }
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
            if data.get(key):
                locations[key] = data[key].get('location')
        self.merge_scalar_fields(['lat', 'lng', 'address', 'city', 'country'], locations, output['location'])

        # merge list field
        self.merge_list_fields(['booking_conditions'], data, output)

        # merge amenities
        amenities = {}
        for key in data:
            if data.get(key):
                amenities[key] = data[key].get('amenities', {})
        self.merge_list_fields(['room', 'general'], amenities, output['amenities'])

        # merge images
        images = {}
        for key in data:
            if data.get(key):
                images[key] = data[key].get('images')
        self.merge_list_fields_with_key(['rooms', 'site', 'amenities'], images, output['images'])

        self.handle_unique_list(output)
        return output

    def merge_scalar_fields(self, scalar_fields: list, data: dict, output: dict):
        for field in scalar_fields:
            for source in self.priority:
                if data.get(source):
                    source_data = data[source]
                    if source_data.get(field):
                        output[field] = source_data[field]

    def merge_list_fields(self, list_fields: list, data: dict, output: dict):
        for field in list_fields:
            output[field] = []
            for source in self.priority:
                if data.get(source):
                    list_merge = data[source].get(field, [])
                    output[field].extend(list_merge or [])

    def merge_list_fields_with_key(self, list_fields: list, data: dict, output: dict):
        # cache all the key value we already added to the list
        known_keys = []
        for field in list_fields:
            output[field] = []
            for source in self.priority:
                if data.get(source) and data[source].get(field) and data[source][field] not in known_keys:
                    output[field].extend(data[source][field])
                    known_keys.append(data[source][field])

    def handle_unique_list(self, data):
        for key, val in data.items():
            if isinstance(val, list):
                if all(isinstance(x, (str, int)) for x in val):
                    data[key] = list(set(val))
                else:
                    data[key] = list({v["link"]: v for v in val}.values())
            elif isinstance(val, dict):
                self.handle_unique_list(data[key])

    def save(self, store, data):
        store.delete_hotel(data['id'])
        store.save_raw_data(store.merge_data_table, [data])


v1_priority_merger = PriorityMerger(['paperflies', 'patagonia', 'acme'])