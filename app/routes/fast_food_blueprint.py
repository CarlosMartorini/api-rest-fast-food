from flask import Blueprint
from app.controllers.fast_food_controller import delete_fats_food, list_all_fast_food, get_fast_food_by_id, delete_fats_food, update_fast_food, create_fast_food

bp = Blueprint('fast_food_bp', __name__, url_prefix='/fast-food')

bp.get('')(list_all_fast_food)
bp.get('/<int:id>')(get_fast_food_by_id)
bp.post('')(create_fast_food)
bp.patch('/<int:id>')(update_fast_food)
bp.delete('/<int:id>')(delete_fats_food)