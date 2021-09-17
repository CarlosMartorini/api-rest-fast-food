from flask import Blueprint
from . import fast_food_blueprint

bp = Blueprint('api_bp', __name__, url_prefix='/api')

bp.register_blueprint(fast_food_blueprint.bp)
