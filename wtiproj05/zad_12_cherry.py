try:
    from cheroot.wsgi import Server as WSGIServer, PathInfoDispatcher
except ImportError:
    from cherrypy.wsgiserver import CherryPyWSGIServer as WSGIServer, WSGIPathInfoDispatcher as PathInfoDispatcher

from zad_7_flask_redis import app



d = PathInfoDispatcher({'/': app})
server = WSGIServer(('127.0.0.1', 9898), d)

if __name__ == '__main__':
   try:
      server.start()
   except KeyboardInterrupt:
      server.stop()