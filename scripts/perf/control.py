import sys, json, urllib.request, urllib.error, datetime, hashlib, hmac

def request(method, url, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            raw = r.read().decode()
            return {'status': r.status, 'body': json.loads(raw) if raw else None}
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            body = json.loads(raw)
        except ValueError:
            body = raw
        return {'status': e.code, 'body': body}

def bucket():
    t = datetime.datetime.now(datetime.timezone.utc)
    stamp = t.strftime('%Y%m%dT%H%M%SZ')
    day = t.strftime('%Y%m%d')
    payload = hashlib.sha256(b'').hexdigest()
    host = '127.0.0.1:9000'
    path = '/comparison'
    headers = f'host:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{stamp}\n'
    signed = 'host;x-amz-content-sha256;x-amz-date'
    canonical = f'PUT\n{path}\n\n{headers}\n{signed}\n{payload}'
    scope = f'{day}/us-east-1/s3/aws4_request'
    msg = f'AWS4-HMAC-SHA256\n{stamp}\n{scope}\n' + hashlib.sha256(canonical.encode()).hexdigest()
    key = b'AWS4benchmark_secret_private'
    for part in [day, 'us-east-1', 's3', 'aws4_request']:
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    sig = hmac.new(key, msg.encode(), hashlib.sha256).hexdigest()
    auth = f'AWS4-HMAC-SHA256 Credential=benchmark_access/{scope}, SignedHeaders={signed}, Signature={sig}'
    req = urllib.request.Request('http://' + host + path, data=b'', method='PUT', headers={'Authorization': auth, 'x-amz-date': stamp, 'x-amz-content-sha256': payload})
    with urllib.request.urlopen(req) as r:
        print(r.status)
if __name__ == '__main__':
    if sys.argv[1] == 'bucket':
        bucket()
    else:
        print(json.dumps(request(sys.argv[1], sys.argv[2], json.loads(sys.argv[3]) if len(sys.argv) > 3 else None)))
