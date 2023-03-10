import unittest
from psycopg2 import DatabaseError
from store.postgres import Postgres
from merger.trust_source_merger import TrustSourceMerger
from loader.rest_api_loader import (
    acme_data_loader,
    patagonia_data_loader,
    paperflies_data_loader,
)
from transformer.field_transformer import acme_transformer, patagonia_transformer, paperflies_transformer
from merger.priority_merger import v1_priority_merger

class Testing(unittest.TestCase):
    def setUp(self) -> None:
        self.trust_source_merger = TrustSourceMerger(
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
        self.acme = {'id': 'iJhz', 'destination_id': 5432, 'name': 'Beach Villas Singapore',
                     'location': {'lat': 1.264751, 'lng': 103.824006, 'address': ' 8 Sentosa Gateway, Beach Villas ',
                                  'city': 'Singapore', 'country': 'SG'}, 'postal_code': '098269',
                     'description': '  This 5 star hotel is located on the coastline of Singapore.',
                     'amenities': {'general': ['Pool', 'BusinessCenter', 'WiFi ', 'DryCleaning', ' Breakfast']}}
        self.patagonia = {'id': 'iJhz', 'destination_id': 5432, 'name': 'Beach Villas Singapore',
                          'location': {'lat': 1.264751, 'lng': 103.824006,
                                       'address': '8 Sentosa Gateway, Beach Villas, 098269', 'city': '', 'country': ''},
                          'description': 'Located at the western tip of Resorts World Sentosa, guests at the Beach Villas are guaranteed privacy while they enjoy spectacular views of glittering waters. Guests will find themselves in paradise with this series of exquisite tropical sanctuaries, making it the perfect setting for an idyllic retreat. Within each villa, guests will discover living areas and bedrooms that open out to mini gardens, private timber sundecks and verandahs elegantly framing either lush greenery or an expanse of sea. Guests are assured of a superior slumber with goose feather pillows and luxe mattresses paired with 400 thread count Egyptian cotton bed linen, tastefully paired with a full complement of luxurious in-room amenities and bathrooms boasting rain showers and free-standing tubs coupled with an exclusive array of ESPA amenities and toiletries. Guests also get to enjoy complimentary day access to the facilities at Asia???s flagship spa ??? the world-renowned ESPA.',
                          'amenities': {
                              'room': ['Aircon', 'Tv', 'Coffee machine', 'Kettle', 'Hair dryer', 'Iron', 'Tub']},
                          'images': {'rooms': [{'description': 'Double room',
                                                'link': 'https://d2ey9sqrvkqdfs.cloudfront.net/0qZF/2.jpg'},
                                               {'description': 'Bathroom',
                                                'link': 'https://d2ey9sqrvkqdfs.cloudfront.net/0qZF/4.jpg'}],
                                     'amenities': [{'description': 'RWS',
                                                    'link': 'https://d2ey9sqrvkqdfs.cloudfront.net/0qZF/0.jpg'},
                                                   {'description': 'Sentosa Gateway',
                                                    'link': 'https://d2ey9sqrvkqdfs.cloudfront.net/0qZF/6.jpg'}]}}
        self.paperflies = {'id': 'iJhz', 'destination_id': 5432, 'name': 'Beach Villas Singapore',
                           'location': {'lat': 0, 'lng': 0, 'address': '8 Sentosa Gateway, Beach Villas, 098269',
                                        'city': '', 'country': 'Singapore'},
                           'description': "Surrounded by tropical gardens, these upscale villas in elegant Colonial-style buildings are part of the Resorts World Sentosa complex and a 2-minute walk from the Waterfront train station. Featuring sundecks and pool, garden or sea views, the plush 1- to 3-bedroom villas offer free Wi-Fi and flat-screens, as well as free-standing baths, minibars, and tea and coffeemaking facilities. Upgraded villas add private pools, fridges and microwaves; some have wine cellars. A 4-bedroom unit offers a kitchen and a living room. There's 24-hour room and butler service. Amenities include posh restaurant, plus an outdoor pool, a hot tub, and free parking.",
                           'amenities': {'general': ['outdoor pool', 'indoor pool', 'business center', 'childcare'],
                                         'room': ['outdoor pool', 'indoor pool', 'business center', 'childcare']},
                           'images': {'rooms': [{'link': 'https://d2ey9sqrvkqdfs.cloudfront.net/0qZF/2.jpg',
                                                 'description': 'Double room'},
                                                {'link': 'https://d2ey9sqrvkqdfs.cloudfront.net/0qZF/3.jpg',
                                                 'description': 'Double room'}], 'site': [
                               {'link': 'https://d2ey9sqrvkqdfs.cloudfront.net/0qZF/2.jpg',
                                'description': 'Double room'},
                               {'link': 'https://d2ey9sqrvkqdfs.cloudfront.net/0qZF/3.jpg',
                                'description': 'Double room'}]}, 'booking_conditions': [
                "All children are welcome. One child under 12 years stays free of charge when using existing beds. One child under 2 years stays free of charge in a child's cot/crib. One child under 4 years stays free of charge when using existing beds. One older child or adult is charged SGD 82.39 per person per night in an extra bed. The maximum number of children's cots/cribs in a room is 1. There is no capacity for extra beds in the room.",
                'Pets are not allowed.', 'WiFi is available in all areas and is free of charge.',
                'Free private parking is possible on site (reservation is not needed).',
                "Guests are required to show a photo identification and credit card upon check-in. Please note that all Special Requests are subject to availability and additional charges may apply. Payment before arrival via bank transfer is required. The property will contact you after you book to provide instructions. Please note that the full amount of the reservation is due before arrival. Resorts World Sentosa will send a confirmation with detailed payment information. After full payment is taken, the property's details, including the address and where to collect keys, will be emailed to you. Bag checks will be conducted prior to entry to Adventure Cove Waterpark. === Upon check-in, guests will be provided with complimentary Sentosa Pass (monorail) to enjoy unlimited transportation between Sentosa Island and Harbour Front (VivoCity). === Prepayment for non refundable bookings will be charged by RWS Call Centre. === All guests can enjoy complimentary parking during their stay, limited to one exit from the hotel per day. === Room reservation charges will be charged upon check-in. Credit card provided upon reservation is for guarantee purpose. === For reservations made with inclusive breakfast, please note that breakfast is applicable only for number of adults paid in the room rate. Any children or additional adults are charged separately for breakfast and are to paid directly to the hotel."]}

        return super().setUp()

    def test_merge_none(self):
        result = self.trust_source_merger.merge(None)
        self.assertEqual(self.trust_source_merger.rule, result)

    def test_merge_acme(self):
        result = self.trust_source_merger.merge(self.acme)

        for key in self.trust_source_merger.rule:
            self.assertIn(key, result)
            self.assertIsNotNone(result[key])

        self.assertDictEqual(result['location'], self.acme['location'])
        self.assertIn('general', result['amenities'])
        self.assertCountEqual(result['amenities']['general'], result['amenities']['general'])

    def test_merge_patagonia(self):
        result = self.trust_source_merger.merge(self.patagonia)

        for key in self.trust_source_merger.rule:
            self.assertIn(key, result)
            self.assertIsNotNone(result[key])

        self.assertDictEqual(result['location'], self.patagonia['location'])
        self.assertIn('room', result['amenities'])
        self.assertCountEqual(result['amenities']['room'], result['amenities']['room'])
        self.assertIn('rooms', result['images'])
        self.assertListEqual(result['images']['rooms'], result['images']['rooms'])
        self.assertIn('amenities', result['images'])
        self.assertListEqual(result['images']['amenities'], result['images']['amenities'])

    def test_merge_paperflies(self):
        result = self.trust_source_merger.merge(self.paperflies)

        for key in self.trust_source_merger.rule:
            self.assertIn(key, result)
            self.assertIsNotNone(result[key])

        self.assertDictEqual(result['location'], self.paperflies['location'])

        self.assertIn('room', result['amenities'])
        self.assertCountEqual(result['amenities']['room'], result['amenities']['room'])
        self.assertIn('general', result['amenities'])
        self.assertCountEqual(result['amenities']['general'], result['amenities']['general'])

        self.assertIn('rooms', result['images'])
        self.assertListEqual(result['images']['rooms'], result['images']['rooms'])
        self.assertIn('site', result['images'])
        self.assertListEqual(result['images']['site'], result['images']['site'])

    def test_merge_sources(self):
        result = self.trust_source_merger.merge(self.acme, self.patagonia, self.paperflies)

        for key in self.trust_source_merger.rule:
            self.assertIn(key, result)
            self.assertIsNotNone(result[key])

        # assert get field location.contry from acme if patagonia and paperflies has not it
        self.assertDictEqual(result['location'], self.acme['location'])

        # assert get field country from acme if patagonia and paperflies has not it
        self.assertEqual(result['location']['country'], self.acme['location']['country'])

        # assert get field booking_conditions from paperflies  if patagonia and paperflies has not it
        self.assertCountEqual(result['booking_conditions'], self.paperflies['booking_conditions'])

        # assert get field amenities.general from acme and paperflies if patagonia has not it
        self.assertLessEqual(
            len(result['amenities']['general']),
            len(self.paperflies['amenities']['general'] + self.acme['amenities']['general'])
        )

        # assert get field amenities.room from patagonia and paperflies if acme has not it
        self.assertLessEqual(
            len(result['amenities']['room']),
            len(self.paperflies['amenities']['room'] + self.patagonia['amenities']['room'])
        )

        # assert get field images.rooms from patagonia and paperflies if acme has not it
        self.assertLessEqual(
            len(result['images']['rooms']),
            len(self.paperflies['images']['rooms'] + self.patagonia['images']['rooms'])
        )

        # assert get field images.amenities from patagonia if acme and paperflies has not it
        self.assertLessEqual(
            len(result['images']['amenities']),
            len(self.patagonia['images']['amenities'])
        )

        # assert get field images.site from paperflies if patagonia and acme has not it
        self.assertLessEqual(
            len(result['images']['site']),
            len(self.paperflies['images']['site'])
        )

    def test_db_error_connecttion(self):
        with self.assertRaises(Exception):
            postgres = Postgres(
                {
                    "db": "merge",
                    "host": "18.139.210.185",
                    "user": "rockshi",
                    "password": "rockship",
                    "merge_data_table": "merge_data",
                    "process_data_table": "process_data",
                    "raw_data_table": "raw_data",
                }
            )

    def test_save_db(self):
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

        result = self.trust_source_merger.merge(self.acme, self.patagonia, self.paperflies)
        self.trust_source_merger.save(postgres, result)

    def test_save_db_error(self):
        with self.assertRaises(DatabaseError) as error:
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

            result = self.trust_source_merger.merge(self.acme, self.patagonia, self.paperflies)
            result['destination_id'] = "abc"
            self.trust_source_merger.save(postgres, result)

    def test_transformer_acme(self):
        # Acme
        key_for_acme = ('id' ,'destination_id' ,'name' ,'location' ,'description' ,'amenities')
        transformed_acme_datas = acme_transformer.convert(acme_data_loader.get_data())

        for transformed_acme_data in transformed_acme_datas:
            for key in key_for_acme:
                self.assertIn(key, transformed_acme_data)
                self.assertIsNotNone(transformed_acme_data[key])

                if key == 'location': 
                    for key_location in ('lat', 'lng', 'address', 'city', 'country'):
                        self.assertIn(key_location, transformed_acme_data['location'])

                if key == 'amenities': 
                    # Check item in list remove empty space
                    self.assertIn('general', transformed_acme_data['amenities'])
                    for amenity in transformed_acme_data['amenities']['general']:
                        self.assertEqual(amenity, amenity.strip())

    def test_transformer_patagonia(self):
        # Patagonia
        key_for_patagonia = ('id','destination_id','name','location','description','amenities','images')
        transformed_patagonia_datas = patagonia_transformer.convert(patagonia_data_loader.get_data())
        
        for transformed_patagonia_data in transformed_patagonia_datas:
            for key in key_for_patagonia:
                self.assertIn(key, transformed_patagonia_data)
                if key != 'description':
                    self.assertIsNotNone(transformed_patagonia_data[key])

                if key == 'location': 
                    for key_location in ('lat', 'lng', 'address'):
                        self.assertIn(key_location, transformed_patagonia_data['location'])

                if key == 'amenities': 
                    # Check item in list remove empty space
                    if not transformed_patagonia_data['amenities'].get('room'):
                        continue
                    for amenity in transformed_patagonia_data['amenities']['room']:
                        self.assertEqual(amenity, amenity.strip())

                if key == 'images': 
                    for key in transformed_patagonia_data['images']:
                        self.assertIn(key, transformed_patagonia_data['images'])
                        self.assertGreater(len(transformed_patagonia_data['images'][key]), 0)

                        for data in transformed_patagonia_data['images'][key]:
                            self.assertIn('link', data)
                            self.assertIsNotNone(data['link'])

                            self.assertIn('description', data)
                            self.assertIsNotNone(data['description'])

    def test_transformer_paperflies(self):
        # Paperflies
        key_for_paperflies = ("id","destination_id","name","location","description","amenities","images","booking_conditions")
        transformed_paperflies_datas = paperflies_transformer.convert(paperflies_data_loader.get_data())
       
        for transformed_paperflies_data in transformed_paperflies_datas:
            for key in key_for_paperflies:
                self.assertIn(key, transformed_paperflies_data)
                self.assertIsNotNone(transformed_paperflies_data[key])

                if key == 'location':
                    for key_location in ('address', 'country'):
                        self.assertIn(key_location, transformed_paperflies_data['location'])
                        self.assertIsNotNone(transformed_paperflies_data['location'][key_location])

                if key == 'amenities': 
                    # Check item in list remove empty space
                    self.assertIn('room', transformed_paperflies_data['amenities'])
                    for amenity in transformed_paperflies_data['amenities']['room']:
                        self.assertEqual(amenity, amenity.strip())

                    self.assertIn('general', transformed_paperflies_data['amenities'])
                    for amenity in transformed_paperflies_data['amenities']['general']:
                        self.assertEqual(amenity, amenity.strip())

                if key == 'booking_conditions': 
                    # Check item in list remove empty space
                    for booking_condition in transformed_paperflies_data['booking_conditions']:
                        self.assertEqual(booking_condition, booking_condition.strip())
                
                if key == 'images': 
                    for key in transformed_paperflies_data['images']:
                        self.assertIn(key, transformed_paperflies_data['images'])
                        self.assertGreater(len(transformed_paperflies_data['images'][key]), 0)

                        for data in transformed_paperflies_data['images'][key]:
                            self.assertIn('link', data)
                            self.assertIsNotNone(data['link'])

                            self.assertIn('description', data)
                            self.assertIsNotNone(data['description'])

    def test_merge_sources_with_class_priority_merger(self):
        result = v1_priority_merger.merge(self.acme['id'], {'paperflies': self.paperflies, 'patagonia': self.patagonia, 'acme': self.acme})

        for key in self.trust_source_merger.rule:
            self.assertIn(key, result)
            self.assertIsNotNone(result[key])

        # assert get field location.contry from acme if patagonia and paperflies has not it
        self.assertDictEqual(result['location'], self.acme['location'])

        # assert get field country from acme if patagonia and paperflies has not it
        self.assertEqual(result['location']['country'], self.acme['location']['country'])

        # assert get field booking_conditions from paperflies  if patagonia and paperflies has not it
        self.assertCountEqual(result['booking_conditions'], self.paperflies['booking_conditions'])

        # assert get field amenities.general from acme and paperflies if patagonia has not it
        self.assertLessEqual(
            len(result['amenities']['general']),
            len(self.paperflies['amenities']['general'] + self.acme['amenities']['general'])
        )

        # assert get field amenities.room from patagonia and paperflies if acme has not it
        self.assertLessEqual(
            len(result['amenities']['room']),
            len(self.paperflies['amenities']['room'] + self.patagonia['amenities']['room'])
        )

        # assert get field images.rooms from patagonia and paperflies if acme has not it
        self.assertLessEqual(
            len(result['images']['rooms']),
            len(self.paperflies['images']['rooms'] + self.patagonia['images']['rooms'])
        )

        # assert get field images.amenities from patagonia if acme and paperflies has not it
        self.assertLessEqual(
            len(result['images']['amenities']),
            len(self.patagonia['images']['amenities'])
        )

        # assert get field images.site from paperflies if patagonia and acme has not it
        self.assertLessEqual(
            len(result['images']['site']),
            len(self.paperflies['images']['site'])
        )