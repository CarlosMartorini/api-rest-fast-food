import psycopg2
from .configs import configs
from csv import DictReader
from app.exceptions.fast_food_exceptions import FastFoodNotFoundError
from psycopg2 import sql

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

    def __init__(self, address, categories, city, country, latitude, longitude, name, postal_code, province, websites):
        self.address = address,
        self.categories = categories,
        self.city = city,
        self.country = country,
        self.latitude = latitude,
        self.longitude = longitude,
        self.name = name,
        self.postal_code = postal_code,
        self.province = province,
        self.websites = websites
    

    @staticmethod
    def get_all():
        conn = psycopg2.connect(**configs)
        
        cur = conn.cursor()

        cur.execute(
            """
                SELECT * FROM fast_food;
            """
        )

        fetch_result = cur.fetchall()

        close_connection(conn, cur)

        serialized_data = [FastFood(fast_food_data).__dixt__ for fast_food_data in fetch_result]

        return serialized_data

    
    @staticmethod
    def get_by_id(id: int):
        conn = psycopg2.connect(**configs)

        cur = conn.cursor()

        cur.execute(
            """
                SELECT * FROM fast_food WHERE id=(%s);
            """,
            (id, )
        )

        fetch_result = cur.fetchone()

        close_connection(conn, cur)

        serialized_data = FastFood(fetch_result).__dict__

        return serialized_data

    
    @staticmethod
    def delete(id: int):
        conn = psycopg2.connect(**configs)

        cur = conn.cursor()

        cur.execute(
            """
                DELETE FROM fast_food WHERE id=(%s) RETURNING *;
            """,
            (id, )
        )

        fetch_result = cur.fetchone()

        close_connection(conn, cur)

        if not fetch_result:
            raise FastFoodNotFoundError(f'Fast Food with id {id} not founded!')
        
        serialized_data = FastFood(fetch_result).__dict__

        return serialized_data


    @staticmethod
    def update(id: int, data):
        conn = psycopg2.connect(**configs)

        cur = conn.cursor()

        columns = [sql.Identifier(key) for key in data.keys()]
        values = [sql.Identifier(value) for value in data.values()]

        query = sql.SQL(
            """
                UPDATE 
                    fast_food
                SET
                    ({columns}) = row({values})
                WHERE   
                    id={id}
                RETURNING *
            """
        ).format(
            id = sql.Literals(str(id)),
            columns = sql.SQL(',').join(columns),
            values = sql.SQL(',').join(values)
        )

        cur.execute(query)

        fetch_result = cur.fetchone()

        close_connection(conn, cur)

        if not fetch_result:
            raise FastFoodNotFoundError(f'Fast Food with id {id} not founded!')
        
        serialized_data = FastFood(fetch_result).__dict__

        return serialized_data


    def save(self):
        conn = psycopg2.connect(**configs)

        cur = conn.cursor()

        columns = [sql.Identifier(key) for key in self.__dict__.keys()]
        values = [sql.Identifier(value) for value in self.__dict__.values()]

        query = sql.SQL(
            """
                INSERT INTO
                    fast_food (id, {columns})
                VALUES
                    (DEFAULT, {values})
                RETURNING *
            """
        ).format(
            columns = sql.SQL(',').join(columns),
            values = sql.SQL(',').join(values)
        )

        cur.execute(query)

        fetch_result = cur.fetchone()

        close_connection(conn, cur)
        
        serialized_data = FastFood(fetch_result).__dict__

        return serialized_data
