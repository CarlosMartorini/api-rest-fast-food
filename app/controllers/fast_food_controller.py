from flask import request, jsonify
from app.models.fast_food_model import FastFood
from app.exceptions.fast_food_exceptions import FastFoodNotFoundError
from psycopg2.errors import UniqueViolation


def list_all_fast_food():
    fast_food_list = FastFood.get_all()
    
    return jsonify(fast_food_list), 200


def get_fast_food_by_id(id: int):
    fast_food = FastFood.get_by_id(id)
    
    return fast_food, 200


def delete_fats_food(id: int):
    try:
        delete_fast_food = FastFood.delete(id)

        return delete_fats_food, 200
    
    except FastFoodNotFoundError as e:
        return {'message': str(e)}, 404


def update_fast_food(id: int):
        try:
            data = request.json

            update_fast_food = FastFood.update(id, data)

            return update_fast_food, 200
        
        except FastFoodNotFoundError as e:
            return {'message': str(e)}, 404


def create_fast_food():
    data = request.json

    try:
        fast_food = FastFood(data)

        new_fast_food = fast_food.save()

        return new_fast_food, 201
    
    except UniqueViolation as e:
        return {'message': str(e).split('\n')[-2]}, 409
