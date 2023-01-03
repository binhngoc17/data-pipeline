from store.postgres import postgres
from flask import Flask, request

app = Flask(__name__)

@app.route("/hotels")
def get_by_hotel_ids():
    hotel_ids = request.args.get('hotel_ids')
    return postgres.get_by_hotel_ids(hotel_ids.split(','))

@app.route("/destinations/<int:destination_id>")
def get_by_destination_id(destination_id: int):
    return postgres.get_by_destination_id(destination_id)