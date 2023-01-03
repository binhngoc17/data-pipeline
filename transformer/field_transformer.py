'''
This class will transform the data from the raw data to the standardize format
This class will also clean the data: remove leading and trailing whitespace in the string
'''

class FieldTransformer:
    def __init__(self, source_name, input_field_map):
        self.source_name = source_name
        self.input_field_map = input_field_map

    def convert(self, data):
        return [
            self.clean(self.replace_keys(old_dict, self.input_field_map))
            for old_dict in data
        ]

    def replace_keys(self, old_dict, key_dict):
        new_dict = {}
        for old_key, new_key in key_dict.items():
            new_keys = new_key.split('.')
            old_keys = old_key.split('.')

            if len(old_keys) == 1 and len(new_keys) == 1:
                # handle rename key
                new_dict[new_keys.pop()] = old_dict.get(old_keys[0], None)
            elif len(old_keys) == 1 and len(new_keys) > 1:
                # handle rename key and convert to dict
                self.handle_rename_key_and_convert_to_dict(
                    old_keys[0], new_keys, old_dict, new_dict
                )
            elif len(old_keys) > 1 and len(new_keys) > 1:
                # handle rename list dictionary
                self.handle_rename_list_dictionary(old_keys, new_keys, old_dict, new_dict)

        return new_dict

    def handle_rename_list_dictionary(self, old_keys, new_keys, old_dict, new_dict):
        value = ''
        is_update = True
        for key in old_keys:
            if isinstance(value, list):
                for x in value:
                    x[new_keys[-1]] = x.pop(key)
                break
            value = old_dict[key]
            old_dict = old_dict[key]
            if is_update:
                new_dict[key] = old_dict
                is_update = False

            new_keys.pop(0)

    def handle_rename_key_and_convert_to_dict(self, old_key, new_keys, old_dict, new_dict):
        value = old_dict.get(old_key, None)
        is_update = True

        for key in list(reversed(new_keys[0:])):
            if key in new_dict:
                new_dict[key].update(value)
                is_update = False
                break
            value = {key: value}

        if is_update:
            new_dict.update(value)

    def clean(self, transformed_item):
        # remove empty space in the string
        for key in ['name', 'description']:
            if transformed_item.get(key):
                transformed_item[key] = transformed_item[key].strip()

        # remove empty space in the string fields of location
        if transformed_item.get('location'):
            for key in ['address', 'city', 'country']:
                if transformed_item['location'].get(key):
                    transformed_item['location'][key] = transformed_item['location'][key].strip()

        # remove empty space in the string fields of amenities
        if transformed_item.get('amenities'):
            for key in ['room', 'general']:
                if transformed_item['amenities'].get(key):
                    transformed_item['amenities'][key] = [item.strip() for item in transformed_item['amenities'][key]]

        # remove empty space in the descriptions of images
        if transformed_item.get('images'):
            for key in ['rooms', 'site', 'amenities']:
                if transformed_item['images'].get(key):
                    for item in transformed_item['images'][key]:
                        if item.get('description'):
                            item['description'] = item['description'].strip()

        return transformed_item

    def save(self, store):
        data = self.convert()
        store.save_raw_data(store.process_data_table, data)

        return data


acme_transformer = FieldTransformer(
    'acme',
    {
        'Id': 'id',
        'DestinationId': 'destination_id',
        'Name': 'name',
        'Latitude': 'location.lat',
        'Longitude': 'location.lng',
        'Address': 'location.address',
        'City': 'location.city',
        'Country': 'location.country',
        'PostalCode': 'postal_code',
        'Description': 'description',
        'Facilities': 'amenities.general',
    },
)

patagonia_transformer = FieldTransformer(
    'patagonia',
    {
        'id': 'id',
        'destination': 'destination_id',
        'name': 'name',
        'lat': 'location.lat',
        'lng': 'location.lng',
        'address': 'location.address',
        'info': 'description',
        'amenities': 'amenities.room',
        'images.rooms.url': 'images.rooms.link',
        'images.amenities.url': 'images.amenities.link',
    },
)

paperflies_transformer = FieldTransformer(
    'paperflies',
    {
        'hotel_id': 'id',
        'destination_id': 'destination_id',
        'hotel_name': 'name',
        'location': 'location',
        'details': 'description',
        'amenities': 'amenities',
        'images.rooms.caption': 'images.rooms.description',
        'images.site.caption': 'images.site.description',
        'booking_conditions': 'booking_conditions'
    },
)

