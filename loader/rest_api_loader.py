import requests


ACME_URL = "https://5f2be0b4ffc88500167b85a0.mockapi.io/suppliers/acme"
PATAGONIA_URL = "https://5f2be0b4ffc88500167b85a0.mockapi.io/suppliers/patagonia"
PAPERFLIES_URL = "https://5f2be0b4ffc88500167b85a0.mockapi.io/suppliers/paperflies"


class JSONApiLoader:
    def __init__(self, source_name, url, key_field):
        self.source_name = source_name
        self.url = url
        self.key_field = key_field

    def get_data(self):
        response = requests.get(self.url)
        if response.status_code == 200:
            return response.json()

    def save(self, store):
        data = self.get_data()
        store.save_raw_data(store.raw_data_table, data)

        return data


acme_data_loader = JSONApiLoader("acme", ACME_URL, "Id")
patagonia_data_loader = JSONApiLoader("patagonia", PATAGONIA_URL, "id")
paperflies_data_loader = JSONApiLoader("paperflies", PAPERFLIES_URL, "hotel_id")
