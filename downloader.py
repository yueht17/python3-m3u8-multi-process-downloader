# coding: utf-8

from gevent import monkey
monkey.patch_all()
from gevent.pool import Pool
import requests
from urllib.parse import urljoin
import os
import time


class Downloader:
    def __init__(self, pool_size, retry=3):
        self.pool = Pool(pool_size)
        self.session = self._get_http_session(pool_size, pool_size, retry)
        self.retry = retry
        self.dir = ''
        self.succed = {}
        self.failed = []
        self.ts_total = 0

    def _get_http_session(self, pool_connections, pool_maxsize, max_retries):
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize,
                                                max_retries=max_retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def run(self, m3u8_url, dir=''):
        self.dir = dir
        if self.dir and not os.path.isdir(self.dir):
            os.makedirs(self.dir)

        r = self.session.get(m3u8_url, timeout=10)
        if r.ok:
            body = r.content.decode()
            if body:
                ts_list = [urljoin(m3u8_url, n.strip()) for n in body.split('\n') if
                           n and not n.startswith("#")]
                if ts_list:
                    self.ts_total = ts_list.__len__()
                    print("[INFO]total sections: ", self.ts_total)
                    self._download(ts_list)
                    self._join_file()
        else:
            print(r.status_code)

    def _download(self, ts_list):
        print("[INFO]download begin")
        self.pool.map(self._worker, ts_list)
        if self.failed:
            ts_list = self.failed
            self.failed = []
            self._download(ts_list)
        print("[INFO]download finished")

    def _worker(self, ts_tuple):
        url = ts_tuple
        index = url.split("_")[-1].replace(".ts", '')
        retry = self.retry
        while retry:
            try:
                r = self.session.get(url, timeout=20)
                if r.ok:
                    file_name = url.split('/')[-1].split('?')[0]
                    with open(os.path.join(self.dir, file_name), 'wb') as f:
                        f.write(r.content)
                    print(
                        "[INFO]" + file_name + " downloaded")
                    self.succed[index] = file_name
                    return
            except Exception as e:
                retry -= 1
                print('[ERROR]%s' % e)
        print('[FAIL]%s' % url)
        self.failed.append((url, index))

    def _join_file(self):
        print('[INFO]join file started')

        index = 1
        outfile = ''
        while index <= self.ts_total:
            file_name = self.succed[str(index)]
            if file_name:
                infile = open(os.path.join(self.dir, file_name), 'rb')
                if not outfile:
                    outfile = open(os.path.join(self.dir, 'all.' + file_name.split('.')[-1]),
                                   'wb')
                outfile.write(infile.read())
                infile.close()
                os.remove(os.path.join(self.dir, file_name))
                index += 1
            else:
                time.sleep(1)
        if outfile:
            outfile.close()
        print('[INFO]join file finished')


if __name__ == '__main__':
    # Example
    # url = "https://vdn-videoback.xuetangx.com/xuetangaudio/record/ProLive-3358952-c8c93c7a/ProLive-3358952-c8c93c7a_2020-02-17-15-20-59_2020-02-17-16-58-00.m3u8"
    # path = ".\\database1\\"
    downloader = Downloader(50)
    downloader.run(url, path)
