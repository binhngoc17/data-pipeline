from loader.rest_api_loader import (
    acme_data_loader,
    patagonia_data_loader,
    paperflies_data_loader,
)
from transformer.field_transformer import acme_transformer, paperflies_transformer, patagonia_transformer
from merger.trust_source_merger import TrustSourceMerger
from store.postgres import Postgres
import json


def beautifull_print(dictionary):
    print(json.dumps(dictionary, indent=4, sort_keys=True))


postgres = Postgres(
    {
        "db": "merge",
        "host": "18.139.210.185",
        "user": "rockship",
        "password": "rockship",
        "merge_data_table": "merge_data",
        "process_data_table": "process_data",
        "raw_data_table": "raw_data",
    }
)

raw_acme_data = acme_data_loader.save(postgres)
transformed_acme_data = acme_transformer.convert(raw_acme_data)

raw_patagonia_data = patagonia_data_loader.save(postgres)
transformed_patagonia_data = patagonia_transformer.convert(raw_patagonia_data)

raw_paperflies_data = paperflies_data_loader.save(postgres)
transformed_paperflies_data = paperflies_transformer.convert(raw_paperflies_data)


for acme in transformed_acme_data:
    trust_source_merger = TrustSourceMerger(
        {
            "id": "",
            "destination_id": 0,
            "name": "",
            "location": {"lat": 0, "lng": 0, "address": "", "city": "", "country": ""},
            "description": "",
            "amenities": {"general": [], "room": []},
            "images": {
                "rooms": [],
                "site": [],
                "amenities": [],
            },
            "booking_conditions": [],
        }
    )
    patagonia = next((x for x in transformed_patagonia_data if acme["id"] == x["id"]), None)
    paperflies = next(
        (x for x in transformed_paperflies_data if acme["id"] == x["id"]), None
    )

    print("======================")
    result = trust_source_merger.merge(acme, patagonia, paperflies)
    trust_source_merger.save(postgres, result)
    beautifull_print(result)
