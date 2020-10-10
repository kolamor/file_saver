import asyncio
import hashlib
import pathlib
import pytest
import re
from aiohttp import ClientSession
from os import environ
from server import RequestInfo


HEADERS = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36"
}


skip_integration = pytest.mark.skipif(not environ.get('integrations'), reason="skip integration")


async def load_pic(session: ClientSession, url: str):
    async with session.get(url, headers=HEADERS) as resp:
        body = await resp.read()
    return body


async def _post(aiohttp_session, file):
    async with aiohttp_session.post('http://0.0.0.0:5555', data=file) as resp:
        body = await resp.json()
        assert resp.status == 200
    l = 128 ** 2  # должен равен чанку чтения сервера и кратное 128 для md5
    chunk = file[:l]
    assert hashlib.md5(chunk).hexdigest() == body['path'].split('/')[-1]
    assert hashlib.md5(file).hexdigest() == body['file_hash_md5']


def test_pars_headers():
    HEADERS = b'POST / HTTP/1.1\r\nHost: 0.0.0.0:5555\r\nAccept: */*\r\nAccept-Encoding: gzip, deflate\r\n' \
              b'User-Agent: Python/3.8 aiohttp/3.6.2\r\nContent-Length: 18328\r\nContent-Type: application/octet-stream\r\n\r\n'
    dict_headers = {'Accept': '*/*',
                    'Accept-Encoding': 'gzip, deflate',
                    'Content-Length': '18328',
                    'Content-Type': 'application/octet-stream',
                    'Host': '0.0.0.0:5555',
                    'User-Agent': 'Python/3.8 aiohttp/3.6.2'}
    request_info = RequestInfo(HEADERS)
    assert request_info.method == 'POST'
    assert request_info.raw_headers == HEADERS
    assert request_info.path == '/'
    assert request_info.is_multipart is False
    assert request_info.headers == dict_headers


@pytest.fixture()
async def aiohttp_session():
    async with ClientSession() as session:
        yield session
    pass


@skip_integration
@pytest.mark.asyncio
async def test_request_get(aiohttp_session):
    with open(pathlib.Path.cwd() / 'storage/test_files' / 'tt.jpg', 'rb') as f:
        data = f.read()
    async with aiohttp_session.post('http://0.0.0.0:5555', data=data) as resp:
        print(resp.status)
        body = await resp.json()
    assert resp.status == 200
    l = 128**2
    chunk = data[:l]
    assert hashlib.md5(chunk).hexdigest() == body['path'].split('/')[-1]
    assert hashlib.md5(data).hexdigest() == body['file_hash_md5']


@skip_integration
@pytest.mark.asyncio
async def test_request_load(aiohttp_session):
    reg = re.compile('<img.+src="([^"]+)')
    async with aiohttp_session.get("https://ru.123rf.com/"
                                   "%D0%A4%D0%BE%D1%82%D0%BE-%D1%81%D0%BE-%D1%81%D1%82%D0%BE%D0%BA%D0%B0/%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D1%8F.html?sti=mcgwcmcxdjsd136jju|",
                                   headers=HEADERS) as resp:
        html = await resp.text()
    urls = reg.findall(html)
    urls = [url for url in list(set(urls)) if url.startswith('http')]
    tasks = []
    for url in urls:
        try:
            task = asyncio.ensure_future(load_pic(aiohttp_session, url))
        except Exception as e:
            continue
        tasks.append(task)
    files = await asyncio.gather(*tasks)
    tasks = []
    for file in files:
        if not file:
            continue
        tasks.append(asyncio.ensure_future(_post(aiohttp_session, file)))
    await asyncio.gather(*tasks)

