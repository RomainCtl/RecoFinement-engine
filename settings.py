import os

PORT = os.environ.get("PORT", 5001)
DEBUG = os.environ.get("FLASK_DEBUG", True)
SECRET_KEY = os.environ.get("FLASK_SECRET", "1234567890")
API_TOKEN = os.environ.get("API_TOKEN", "FOOBAR1")
REDIS_URL = os.environ.get("REDIS_URL", "redis://%s:%s" %
                           (os.environ.get("REDIS_HOST", "localhost"), os.environ.get("REDIS_PORT", "6379")))

DB_URI = "postgresql://%s:%s@%s:%s/%s" % (os.environ["DB_USER_LOGIN"], os.environ["DB_USER_PASSWORD"],
                                          os.environ["DB_HOST"], os.environ["DB_PORT"], os.environ["DB_NAME"])

DEFAULT_RENDERERS = [
    "flask_api.renderers.JSONRenderer"
]
DEFAULT_PARSERS = [
    "flask_api.parsers.JSONParser",
    "flask_api.parsers.URLEncodedParser",
]
