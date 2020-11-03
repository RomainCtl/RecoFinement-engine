from flask import request, current_app, abort
from flask_api import FlaskAPI
from flask_api.exceptions import PermissionDenied
from functools import wraps

#: Flask application
app = FlaskAPI(__name__)
app.config.from_object("settings")


# Not much security because only the recofinement api service will be able to use this api
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
    eng = Popularity()
    eng.start()
    return {"started": True}, 202


@app.route("/content_similarities/train", methods=["POST"])
@token_auth
def content_similarities_train():
    from src.engines import ContentSimilarities
    eng = ContentSimilarities()
    eng.start()
    return {"started": True}, 202
