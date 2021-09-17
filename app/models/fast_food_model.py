import psycopg2
from .configs import configs


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
    conn = psycopg2.connect(**configs)

    cur = conn.cursor()

    cur.execute(
        """
            COPY fast_food FROM './data/fast_food_restaurant_us.csv' DELIMITER ',' CSV HEADER;
        """
    )

    close_connection(conn, cur)


class FastFood():

    ...


