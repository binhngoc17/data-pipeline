import psycopg2
from psycopg2 import sql
from typing import TypedDict
import json


class PostgresConfig(TypedDict):
    host: str
    db: str
    user: str
    password: str
    raw_data_table: str
    process_data_table: str
    merge_data_table: str


class Postgres:
    def __init__(self, config: PostgresConfig):
        self.config = config
        self.raw_data_table = config["raw_data_table"]
        self.process_data_table = config["process_data_table"]
        self.merge_data_table = config["merge_data_table"]

        self.create_table()

    def connect_db(self):
        try:
            return psycopg2.connect(
                host=self.config["host"],
                database=self.config["db"],
                user=self.config["user"],
                password=self.config["password"],
            )
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise psycopg2.DatabaseError("Connect database error")

    def save_raw_data(self, table_name, data):
        """insert multiple row into the table"""
        try:
            conn = self.connect_db()
            cur = conn.cursor()

            columns = [x.lower() for x in list(data[0].keys())]
            values = [kvalue.values() for kvalue in data]
            records = []
            for value in values:
                t = ()
                for item in value:
                    if isinstance(item, (dict, list)):
                        item = json.dumps(item)
                    t = t + (item,)
                records.append(t)

            self.add_row(cur, table_name, columns, records)

            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise psycopg2.DatabaseError("Insert data error")
        finally:
            if conn is not None:
                conn.close()

    def add_row(self, cur, table_name, columns, records):
        query_string = (
            sql.SQL("INSERT INTO {} ({}) VALUES {}")
            .format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(records)),
            )
            .as_string(cur)
        )
        query = cur.mogrify(query_string, records).decode("utf-8")
        cur.execute(query)

    def create_table(self):
        """create tables in the PostgreSQL database"""
        commands = (
            f"""
            CREATE TABLE IF NOT EXISTS {self.raw_data_table} (
                ID VARCHAR ( 255 ),
                DestinationId INT,
                NAME VARCHAR ( 255 ),
                Latitude VARCHAR ( 255 ),
                Longitude VARCHAR ( 255 ),
                Address VARCHAR ( 255 ),
                City VARCHAR ( 255 ),
                Country VARCHAR ( 255 ),
                PostalCode VARCHAR ( 255 ),
                Description TEXT,
                Facilities JSON,
                --
                destination INT,
                lat VARCHAR ( 255 ),
                lng VARCHAR ( 255 ),
                info TEXT,
                images JSON,
                --
                hotel_id VARCHAR ( 255 ),
                destination_id INT,
                hotel_name VARCHAR ( 255 ),
                LOCATION JSON,
                details TEXT,
                amenities JSON,
                booking_conditions JSON
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {self.process_data_table} (
                id VARCHAR ( 255 ),
                destination_id INT,
                name VARCHAR ( 255 ),
                postal_code VARCHAR ( 255 ),
                description TEXT,
                amenities JSON,
                --
                info TEXT,
                images JSON,
                --
                location JSON,
                details TEXT,
                booking_conditions JSON
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {self.merge_data_table} (
                id VARCHAR ( 255 ),
                destination_id INT,
                name VARCHAR ( 255 ),
                location JSON,
                description TEXT,
                amenities JSON,
                images JSON,
                booking_conditions JSON
            )
            """,
        )
        try:
            conn = self.connect_db()
            cur = conn.cursor()
            # create table one by one
            for command in commands:
                cur.execute(command)
            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()