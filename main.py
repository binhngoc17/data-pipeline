from loader.rest_api_loader import (
    acme_data_loader,
    patagonia_data_loader,
    paperflies_data_loader,
)
from transformer.field_transformer import acme_transformer, paperflies_transformer, patagonia_transformer
from merger.priority_merger import v1_priority_merger
from store.postgres import postgres
import json

def beautifull_print(dictionary):
    print(json.dumps(dictionary, indent=4, sort_keys=True))


raw_acme_data = acme_data_loader.save(postgres)
transformed_acme_data = acme_transformer.convert(raw_acme_data)

raw_patagonia_data = patagonia_data_loader.save(postgres)
transformed_patagonia_data = patagonia_transformer.convert(raw_patagonia_data)

raw_paperflies_data = paperflies_data_loader.save(postgres)
transformed_paperflies_data = paperflies_transformer.convert(raw_paperflies_data)

all_hotel_ids = [acme['id'] for acme in transformed_acme_data] + [patagonia['id'] for patagonia in transformed_patagonia_data] + [paperflies['id'] for paperflies in transformed_paperflies_data]
all_hotel_ids = set(all_hotel_ids)

for hotel_id in all_hotel_ids:
    acme = next((x for x in transformed_acme_data if hotel_id == x['id']), None)
    patagonia = next((x for x in transformed_patagonia_data if hotel_id == x['id']), None)
    paperflies = next((x for x in transformed_paperflies_data if hotel_id == x['id']), None)

    print('======================')
    result = v1_priority_merger.merge(hotel_id, {'paperflies': paperflies, 'patagonia': patagonia, 'acme': acme})
    v1_priority_merger.save(postgres, result)
    beautifull_print(result)


