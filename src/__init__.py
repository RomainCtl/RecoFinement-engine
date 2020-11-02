from flask import request, current_app, abort
from flask_api import FlaskAPI
from flask_api.exceptions import PermissionDenied
from functools import wraps

#: Flask application
app = FlaskAPI(__name__)
app.config.from_object("settings")


def token_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.headers.get("X-API-TOKEN", None) != current_app.config["API_TOKEN"]:
            raise PermissionDenied()
        return f(*args, **kwargs)
    return decorated_function


@app.route("/popularity/train", methods=["POST"])
@token_auth
def popularity_train():
    from src.engines import Popularity
    p = Popularity()
    p.start()
    return {"started": True}, 202
