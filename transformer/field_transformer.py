class FieldTransformer:
    def __init__(self, source_name, input_field_map):
        self.source_name = source_name
        self.input_field_map = input_field_map

    def convert(self):
        return [
            self.replace_keys(old_dict, self.input_field_map)
            for old_dict in self.source_name
        ]

    def replace_keys(self, old_dict, key_dict):
        new_dict = {}
        for old_key, new_key in key_dict.items():
            new_keys = new_key.split(".")
            old_keys = old_key.split(".")

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
        value = ""
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

    def save(self, store):
        data = self.convert()
        store.save_raw_data(store.process_data_table, data)

        return data
