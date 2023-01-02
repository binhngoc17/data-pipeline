from loader.rest_api_loader import (
    acme_data_loader,
    patagonia_data_loader,
    paperflies_data_loader,
)
from transformer.field_transformer import FieldTransformer
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

acme_data = acme_data_loader.save(postgres)
transformer_acme = FieldTransformer(
    acme_data,
    {
        "Id": "id",
        "DestinationId": "destination_id",
        "Name": "name",
        "Latitude": "location.lat",
        "Longitude": "location.lng",
        "Address": "location.address",
        "City": "location.city",
        "Country": "location.country",
        "PostalCode": "postal_code",
        "Description": "description",
        "Facilities": "amenities.general",
    },
).save(postgres)

patagonia_data = patagonia_data_loader.save(postgres)
transformer_patagonia = FieldTransformer(
    patagonia_data,
    {
        "id": "id",
        "destination": "destination_id",
        "name": "name",
        "lat": "location.lat",
        "lng": "location.lng",
        "address": "location.address",
        "info": "description",
        "amenities": "amenities.room",
        "images.rooms.url": "images.rooms.link",
        "images.amenities.url": "images.amenities.link",
    },
).save(postgres)


paperflies_data = paperflies_data_loader.save(postgres)
transformer_paperflies = FieldTransformer(
    paperflies_data,
    {
        "hotel_id": "id",
        "destination_id": "destination_id",
        "hotel_name": "name",
        "location": "location",
        "details": "description",
        "amenities": "amenities",
        "images.rooms.caption": "images.rooms.description",
        "images.site.caption": "images.site.description",
        "booking_conditions": "booking_conditions"
    },
).save(postgres)


for acme in transformer_acme:
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
    patagonia = next((x for x in transformer_patagonia if acme["id"] == x["id"]), None)
    paperflies = next(
        (x for x in transformer_paperflies if acme["id"] == x["id"]), None
    )

    print("======================")
    result = trust_source_merger.merge(acme, patagonia, paperflies)
    trust_source_merger.save(postgres, result)
    beautifull_print(result)
