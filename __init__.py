from gevent import monkey; monkey.patch_all()
from gevent.pywsgi import WSGIServer
from sibyl_api import app

http_server = WSGIServer(('0.0.0.0', 5000), app)
http_server.serve_forever()