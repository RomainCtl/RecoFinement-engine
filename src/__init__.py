from .recommend import Recommend, RecommendUser, start_popularity_engine, start_similarities_engine, start_collaborative_engine, start_from_profile_engine, start_from_similar_content_engine, start_from_profile_engine_for_group, start_from_similar_content_engine_for_group

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


@app.route("/up")
def up():
    return {"up": True}, 200


@app.route("/popularity/train", methods=["PUT"])
@token_auth
def popularity_train():
    start_popularity_engine(wait=False)
    return {"started": True}, 202


@app.route("/content_similarities/train", methods=["PUT"])
@token_auth
def content_similarities_train():
    start_similarities_engine(wait=False)
    return {"started": True}, 202


@app.route("/collaborative_filtering/train", methods=["PUT"])
@token_auth
def collaborative_filtering_train():
    start_collaborative_engine(wait=False)
    return {"started": True}, 202


@app.route("/from_profile/train", methods=["PUT"])
@token_auth
def from_profile_train():
    start_from_profile_engine(wait=False)
    return {"started": True}, 202


@app.route("/from_similar_content/train", methods=["PUT"])
@token_auth
def from_similar_content_train():
    start_from_similar_content_engine(wait=False)
    return {"started": True}, 202


@app.route("/from_profile/group/train", methods=["PUT"])
@token_auth
def from_group_profile_train():
    start_from_profile_engine_for_group(wait=False)
    return {"started": True}, 202


@app.route("/from_similar_content/group/train", methods=["PUT"])
@token_auth
def from_similar_content_group_train():
    start_from_similar_content_engine_for_group(wait=False)
    return {"started": True}, 202


@app.route("/recommend/", methods=["PUT"])
@token_auth
def recommend():
    eng = Recommend()
    eng.start()
    return {"started": True}, 202


@app.route("/recommend/<uuid:user_uuid>", methods=["PUT"])
@token_auth
def recommend_user(user_uuid):
    eng = RecommendUser(user_uuid=user_uuid)
    eng.start()
    return {"started": True}, 202
