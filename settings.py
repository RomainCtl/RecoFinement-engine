import os

PORT = os.environ.get("SERVICE_PORT", 4041)
DEBUG = os.environ.get("FLASK_DEBUG", True)
SECRET_KEY = os.environ.get("FLASK_SECRET", "1234567890")
API_TOKEN = os.environ.get("API_TOKEN", "FOOBAR1")

DB_URI = "postgresql://%s:%s@%s:%s/%s" % (
    os.environ.get("DB_USER_LOGIN", "reco_usr"),
    os.environ.get("DB_USER_PASSWORD", "reco_pwd"),
    os.environ.get("DB_HOST", "localhost"),
    os.environ.get("DB_PORT", "5432"),
    os.environ.get("DB_NAME", "recofinement")
)

DEFAULT_RENDERERS = [
    "flask_api.renderers.JSONRenderer"
]
DEFAULT_PARSERS = [
    "flask_api.parsers.JSONParser",
    "flask_api.parsers.URLEncodedParser",
]
