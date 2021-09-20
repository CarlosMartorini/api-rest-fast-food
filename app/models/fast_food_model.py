import psycopg2
from .configs import configs
from csv import DictReader

def close_connection(conn, cur):
    conn.commit()
    cur.close()
    conn.close()


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
                country VARCHAR(5),
                latitude VARCHAR(15),
                longitude VARCHAR(15),
                name VARCHAR(255),
                postal_code INT,
                province VARCHAR(5),
                websites TEXT
            );
        """
    )

    close_connection(conn, cur)


def populate_table():
    with open('fast_food_restaurants_us.csv', 'r') as file:
        
        data_file = DictReader(file)

        for row in data_file:

            conn = psycopg2.connect(**configs)

            cur = conn.cursor()

            query = """
                INSERT INTO fast_food
                    (id, address, categories, city, country, latitude, longitude, name, postal_code, province, websites)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,)
            """
            params = (
                row['id'],
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


class FastFood():

    ...


