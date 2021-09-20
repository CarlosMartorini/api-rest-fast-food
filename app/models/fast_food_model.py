from flask.json import jsonify
import psycopg2
from .configs import configs
from csv import DictReader
from app.exceptions.fast_food_exceptions import FastFoodNotFoundError
from psycopg2 import sql
from typing import Union


def close_connection(conn, cur):
    conn.commit()
    cur.close()
    conn.close()


class FastFood():

    def __init__(self, fields: Union[tuple, dict]) -> None:
        if type(fields) is dict:
            for k, v in fields.items():
                setattr(self, k, v)
    

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

        return fetch_result

    
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

        return jsonify(fetch_result)

    
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
        values = [sql.Literal(value) for value in data.values()]

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
            id = sql.Literal(str(id)),
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

        data = self.__dict__

        columns = [sql.Identifier(key) for key in data.keys()]
        values = [sql.Literal(value) for value in data.values()]

        query = sql.SQL(
            """
                INSERT INTO fast_food 
                    (id, {columns})
                VALUES
                    (DEFAULT, {values})
                RETURNING *
            """
        ).format(columns=sql.SQL(',').join(columns), 
                values=sql.SQL(',').join(values))

        cur.execute(query)

        fetch_result = cur.fetchone()

        close_connection(conn, cur)
        
        serialized_data = FastFood(fetch_result).__dict__

        return serialized_data
