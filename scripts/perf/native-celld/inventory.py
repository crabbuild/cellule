#!/usr/bin/env python3
"""Read-only S3 inventory for the private synthetic RustFS benchmark bucket."""
import collections
import datetime
import hashlib
import hmac
import json
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


def inventory(prefix):
    host, path = '127.0.0.1:9000', '/comparison'
    token, objects = None, []
    while True:
        query = {'list-type': '2', 'prefix': prefix, 'max-keys': '1000'}
        if token is not None:
            query['continuation-token'] = token
        encoded = '&'.join(urllib.parse.quote(k, safe='') + '=' + urllib.parse.quote(v, safe='')
                           for k, v in sorted(query.items()))
        now = datetime.datetime.now(datetime.timezone.utc)
        day, stamp = now.strftime('%Y%m%d'), now.strftime('%Y%m%dT%H%M%SZ')
        payload = hashlib.sha256(b'').hexdigest()
        headers = f'host:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{stamp}\n'
        signed = 'host;x-amz-content-sha256;x-amz-date'
        canonical = f'GET\n{path}\n{encoded}\n{headers}\n{signed}\n{payload}'
        scope = f'{day}/us-east-1/s3/aws4_request'
        message = f'AWS4-HMAC-SHA256\n{stamp}\n{scope}\n' + hashlib.sha256(canonical.encode()).hexdigest()
        key = b'AWS4benchmark_secret_private'
        for part in (day, 'us-east-1', 's3', 'aws4_request'):
            key = hmac.new(key, part.encode(), hashlib.sha256).digest()
        signature = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
        request = urllib.request.Request(f'http://{host}{path}?{encoded}', headers={
            'Authorization': f'AWS4-HMAC-SHA256 Credential=benchmark_access/{scope}, SignedHeaders={signed}, Signature={signature}',
            'x-amz-date': stamp, 'x-amz-content-sha256': payload})
        with urllib.request.urlopen(request, timeout=90) as response:
            root = ET.fromstring(response.read())
        for item in root.findall('{*}Contents'):
            objects.append(dict(key=item.findtext('{*}Key'), bytes=int(item.findtext('{*}Size'))))
        if root.findtext('{*}IsTruncated') != 'true':
            break
        token = root.findtext('{*}NextContinuationToken')
        if not token:
            raise RuntimeError('truncated inventory lacks continuation token')
    families = collections.defaultdict(lambda: dict(objects=0, bytes=0))
    for obj in objects:
        key = obj['key']
        suffix = key.rsplit('/', 1)[-1].rsplit('.', 1)[-1]
        family = suffix if suffix in ('ltx', 'index', 'dir', 'root', 'pack', 'spack', 'json') else 'other'
        families[family]['objects'] += 1
        families[family]['bytes'] += obj['bytes']
    return dict(prefix=prefix, object_count=len(objects), bytes=sum(o['bytes'] for o in objects),
                families=dict(families), objects=objects)


if __name__ == '__main__':
    print(json.dumps(inventory(sys.argv[1])))
