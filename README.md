`Сервер на Python 3.6+`

`python main.py` - стандартные настройки `0.0.0.0:5555`

или `python main.py --host=0.0.0.0 --port=5555`

#### Релизации:

asyncio сервер с записью файлов в потока, с возможностью обработки больших файлов и буферизацией

```python
from aiohttp import ClientSession
import hashlib

data: bytes = b'...'

async with ClientSession() as  session:
    async with session.post('http://0.0.0.0:5555', data=data) as resp:
        json = await resp.json()

len_chunk = 128**2
assert json['path'].split('/')[-1] == hashlib.md5(data[:len_chunk]).hexdigest() 
assert json['file_hash_md5'] == hashlib.md5(data).hexdigest()
```
ответ:
имя файла берется hash md5 первых 128**2 байт
после обработки возвращает полный hash файла и путь

```json
{
   "path":"/home/kola/PycharmProjects/aio_upload_file/storage/47a65a7ba7dd2e63dcd83e923fc27138",
   "file_hash_md5":"3d4ac3e5f8258a5fc1ab8d63fc3ea278"
}
```

#### P.S
для тестового сценария установить
 
`pip install -r requirements_test.txt`

запустить

`env integrations=t pytest tests.py`

будет загруженно 100+ файлов в папку `storage`