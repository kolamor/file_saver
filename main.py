import argparse
import asyncio
import logging
from server import *

parser = argparse.ArgumentParser(description='app api_es')
parser.add_argument('--host', help='Host to listen', default='0.0.0.0')
parser.add_argument('--port', help='Port to accept connections', default='5555')
parser.add_argument('-c', '--config', type=argparse.FileType('r'), 	help='Path to configuration file')
parser.add_argument('--uvloop', action='store_true', help='Enable uvloop')
parser.add_argument('-ll', '--logginglevel', help='Logging level', default='INFO')

args = parser.parse_args()
if args.uvloop:
    print('Start with uvloop')
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        print("uvloop is not install")


async def start():
    print(f'start server {args.host}:{args.port}')
    try:
        srv = await asyncio.start_server(handler, host=args.host, port=args.port, )
        create_task(stats())
        await srv.serve_forever()
    finally:
        await asyncio.sleep(1)
        print('stop')


if __name__ == '__main__':
    logging.basicConfig(level=getattr(logging, args.logginglevel))
    try:
        asyncio.run(start(), debug=True if args.logginglevel == 'DEBUG' else False)
    except KeyboardInterrupt as e:
        print('----')
