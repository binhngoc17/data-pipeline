class TrustSourceMerger:
    def __init__(self, rule):
        self.rule = rule

    def merge(self, *sources):
        result = {}
        for x in sources[::-1]:
            result = self.handle_merge(self.rule, x)
            self.handle_unique_list(result)
        return result

    def handle_merge(self, destination, source):
        new_dict = destination.copy()
        if not source:
            return destination

        for key in source:
            if key not in destination:
                continue

            if isinstance(destination[key], dict) and isinstance(source[key], dict):
                # If the key for both `destination` and `source` are both Mapping types (e.g. dict), then recurse.
                new_dict[key] = self.handle_merge(destination[key], source[key])
            elif isinstance(destination[key], list) and isinstance(source[key], list):
                new_dict[key].extend(source[key])
            elif destination[key] is source[key]:
                pass
            else:
                new_dict[key] = source[key]
        return new_dict

    def handle_unique_list(self, data):
        for key, val in data.items():
            if isinstance(val, list):
                if all(isinstance(x, (str, int)) for x in val):
                    data[key] = list(set(val))
                else:
                    data[key] = list({v["link"]: v for v in val}.values())
            elif isinstance(val, dict):
                self.handle_unique_list(data[key])
            else:
                pass

    def save(self, store, data):
        store.save_raw_data(store.merge_data_table, [data])
