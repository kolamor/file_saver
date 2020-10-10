import asyncio
import json
import logging
import sys
import weakref
import email
import hashlib
import io
import pathlib
from asyncio import StreamReader, StreamWriter
from typing import Optional, Union, Any, Awaitable


if sys.version_info < (3, 7)[:2]:
    from asyncio import ensure_future as create_task
else:
    from asyncio import create_task


STORAGE_URL = pathlib.Path.cwd() / 'storage'

WEAK_READERS = weakref.WeakSet()
WEAK_WRITERS = weakref.WeakSet()
WEAK_FILE_STREAM = weakref.WeakSet()

logger = logging.getLogger('server')

__all__ = ('handler', 'stats', 'create_task', 'RequestInfo')

def weakref_info(coro): # noqa
    async def wrapper(reader: StreamReader, writer: StreamWriter, req_info: RequestInfo):
        print(req_info.raw_headers)
        WEAK_READERS.add(reader)
        WEAK_WRITERS.add(writer)
        logger.info(f'readers: {len(WEAK_READERS)} :: writers {len(WEAK_WRITERS)} :: file stream {len(WEAK_FILE_STREAM)}')
        res = await coro(reader, writer, req_info)
        return res
    return wrapper


async def stats():
    while True:
        logger.info(f'readers: {len(WEAK_READERS)} :: writers {len(WEAK_WRITERS)} :: file streams: {len(WEAK_FILE_STREAM)}')
        await asyncio.sleep(10)


class ServerError(Exception):
    pass


class RequestInfo:
    """Парсер заголовков"""
    __method : str
    __path: str
    __protocol: str
    __headers: dict
    __multipart_type: str

    def __init__(self, raw_headers: bytes):
        self.__raw_headers: bytes = raw_headers
        self._create_headers()

    def _create_headers(self) -> None:
        request_line, headers_alone = str(self.__raw_headers, 'utf-8').split('\r\n', 1)
        message = email.message_from_file(io.StringIO(headers_alone))
        self.__headers = dict(message.items())
        self.__method, self.__path, self.__protocol = request_line.split(' ')

    @classmethod
    async def init(cls, reader: StreamReader) -> 'RequestInfo':
        raw_headers = await cls._get_raw_header_from_stream(reader=reader)
        self = cls(raw_headers=raw_headers)
        return self

    @classmethod
    async def _get_raw_header_from_stream(cls, reader: StreamReader, timeout: int = 5) -> bytes:
        raw_headers = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), timeout)
        return raw_headers

    @property
    def raw_headers(self) -> bytes:
        return self.__raw_headers

    @property
    def headers(self) -> dict:
        return self.__headers.copy()

    @property
    def method(self) -> str:
        return self.__method

    @property
    def path(self) -> str:
        return self.__path

    @property
    def is_multipart(self) -> bool:
        content_type: str = self.__headers.get('Content-Type', None)
        if content_type.lower().startswith('multipart/'):
            _, self.__multipart_type = content_type.split('/')
            return True
        return False

    @property
    def multipart_type(self) -> Optional[str]:
        if not self.__multipart_type:
            if self.is_multipart:
                return self.__multipart_type
        return


class Error:
    """сигнал пилюля потоку"""
    def __init__(self, message: Any = None):
        self.message = message


class End(Error):
    """сигнал пилюля потоку"""
    pass


class FileStream:

    # TODO лучше использовать потокозащищенную очередь, например Janus

    def __init__(self, buffer: int = 100):
        self.in_queue = asyncio.Queue(buffer)
        self.out_queue = asyncio.Queue(buffer)

    async def get_in_queue_chunk(self) -> Union[bytes, End, Error]:
        res = await self.in_queue.get()
        self.in_queue.task_done()
        return res

    async def put_in_queue_chunk(self, chunk: Union[bytes, End, Error]) -> None:
        await self.in_queue.put(chunk)

    async def get_out_queue_chunk(self) -> Union[bytes, End, Error]:
        res = await self.out_queue.get()
        self.out_queue.task_done()
        return res

    async def put_out_queue_chunk(self, chunk: Union[bytes, dict, Error]) -> None:
        await self.out_queue.put(chunk)


async def file_writer(stream: FileStream) -> None:
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, _file_writer, stream, loop)
    except Exception as e:
        logger.error(f'file writer exception, {e} --- {e.args}')
        await stream.put_out_queue_chunk(Error())


def _file_writer(stream: FileStream, loop: asyncio.BaseEventLoop) -> None:
    def call_coro(coroutine: Awaitable, timeout: int = 20) -> Any:
        """Вызываем корутину не из основного потока c таймаутом"""
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        try:
            result = future.result(timeout=timeout)
        except asyncio.TimeoutError:
            future.cancel()
            raise
        except Exception as e:
            raise e
        return result

    coro = stream.get_in_queue_chunk()
    result = call_coro(coro)
    if isinstance(result, bytes):
        file_hash = hashlib.md5()
        file_hash.update(result)
        # TODO реализовать обработку только одного файла c совпадающимися хешами
    else:
        return

    path = f'{STORAGE_URL}/{file_hash.hexdigest()}'
    with open(path, 'wb') as f:
        f.write(result)

        while not isinstance(result, (End, Error)):
            coro = stream.get_in_queue_chunk()
            result = call_coro(coro)
            if isinstance(result, End):
                continue
            file_hash.update(result)
            f.write(result)

    coro = stream.put_out_queue_chunk({'path': path, 'file_hash_md5': file_hash.hexdigest()})
    call_coro(coro)


async def handler(reader: StreamReader, writer: StreamWriter) -> None:
    try:
        req_info = await RequestInfo.init(reader=reader)
        if req_info.method == 'POST':
            if req_info.is_multipart:
                pass
                # TODO релиозовать mltipart загрузку https://tools.ietf.org/html/rfc2046
            else:
                await _handler(reader=reader, writer=writer, req_info=req_info)
        else:
            writer.write(
                b'HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n400 Bad Request\r\n\r\n')

    except Exception as e:
        logger.error(f'{e} --- {e.args}')
        writer.write(
            b'HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\nInternal Server Error\r\n\r\n')
    finally:
        await asyncio.wait_for(writer.drain(), 2)
        writer.close()


@weakref_info
async def _handler(reader: StreamReader, writer: StreamWriter, req_info: RequestInfo) -> None:
    length_body = 0
    # создаем очереди для записи в потоке
    file_stream = FileStream()
    WEAK_FILE_STREAM.add(file_stream)
    # создаем поток
    create_task(file_writer(stream=file_stream))
    while not reader.at_eof():
        chunk = await reader.read(128**2)
        length_body += len(chunk)
        if chunk == b'':
            break
        await file_stream.put_in_queue_chunk(chunk)
        if length_body == int(req_info.headers['Content-Length']):
            break

    await file_stream.put_in_queue_chunk(End())
    # Ждем обработки файла
    file_info = await file_stream.get_out_queue_chunk()
    if isinstance(file_info, Error):
        raise ServerError('error: write file')
    if isinstance(file_info, dict):
        # TODO где-нибудь сохранить hash_file и path
        logger.info(file_info)
        writer.write(
            f'HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nConnection: close\r\n\r\n{json.dumps(file_info)}\r\n\r\n'.encode('utf-8'))
