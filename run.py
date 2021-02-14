from src import app, socketio
from settings import PORT

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=PORT)
