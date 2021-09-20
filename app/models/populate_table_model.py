import psycopg2
from .configs import configs
from csv import DictReader

csv_file = 'app/models/fast_food_restaurants_us.csv'


def close_connection(conn, cur):
    conn.commit()
    cur.close()
    conn.close()


def drop_database():
    conn = psycopg2.connect(**configs)

    cur = conn.cursor()

    cur.execute(
        """
            DROP DATABASE fast_food;
        """
    )

    close_connection(conn, cur)


def create_database():
    conn = psycopg2.connect(**configs)

    cur = conn.cursor()

    cur.execute(
        """
            CREATE DATABASE fast_food;
        """
    )

    close_connection(conn, cur)


def create_table():
    conn = psycopg2.connect(**configs)

    cur = conn.cursor()

    cur.execute(
        """
            CREATE TABLE IF NOT EXISTS fast_food (
                id BIGSERIAL PRIMARY KEY,
                address VARCHAR(255),
                categories VARCHAR(255),
                city VARCHAR(255),
                country VARCHAR(255),
                latitude VARCHAR(255),
                longitude VARCHAR(255),
                name VARCHAR(255),
                postal_code VARCHAR(255),
                province VARCHAR(255),
                websites TEXT
            );
        """
    )

    close_connection(conn, cur)


def populate_table():
    with open(csv_file, 'r', encoding='utf-8') as file:
        
        data_file = DictReader(file)

        for row in data_file:

            print(row)

            conn = psycopg2.connect(**configs)

            cur = conn.cursor()

            query = """
                INSERT INTO fast_food
                    (id, address, categories, city, country, latitude, longitude, name, postal_code, province, websites)
                VALUES
                    (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                row['address'],
                row['categories'],
                row['city'],
                row['country'],
                row['latitude'],
                row['longitude'],
                row['name'],
                row['postal_code'],
                row['province'],
                row['websites']
            )

            cur.execute(query, params)

            close_connection(conn, cur)

