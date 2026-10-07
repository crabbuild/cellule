import datetime, hashlib, hmac, json, time, urllib.request, urllib.error
started = time.monotonic()
stable = 0
attempts = 0
while time.monotonic() - started < 60:
    attempts += 1
    now = datetime.datetime.now(datetime.timezone.utc)
    date = now.strftime('%Y%m%d')
    stamp = now.strftime('%Y%m%dT%H%M%SZ')
    payload = hashlib.sha256(b'').hexdigest()
    query = 'list-type=2&max-keys=1'
    host = '127.0.0.1:9000'
    headers = 'host:' + host + '\nx-amz-content-sha256:' + payload + '\nx-amz-date:' + stamp + '\n'
    signed = 'host;x-amz-content-sha256;x-amz-date'
    canonical = 'GET\n/comparison\n' + query + '\n' + headers + '\n' + signed + '\n' + payload
    scope = date + '/us-east-1/s3/aws4_request'
    message = 'AWS4-HMAC-SHA256\n' + stamp + '\n' + scope + '\n' + hashlib.sha256(canonical.encode()).hexdigest()

    def mac(key, value):
        return hmac.new(key, value.encode(), hashlib.sha256).digest()
    key = mac(mac(mac(mac(b'AWS4benchmark_secret_private', date), 'us-east-1'), 's3'), 'aws4_request')
    signature = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
    request = urllib.request.Request('http://' + host + '/comparison?' + query, headers={'x-amz-date': stamp, 'x-amz-content-sha256': payload, 'Authorization': 'AWS4-HMAC-SHA256 Credential=benchmark_access/' + scope + ', SignedHeaders=' + signed + ', Signature=' + signature})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            body = response.read()
            ready = response.status == 200 and b'ListBucketResult' in body
        stable = stable + 1 if ready else 0
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        stable = 0
    if stable >= 3:
        print(json.dumps({'bucket_list_ready': True, 'attempts': attempts, 'seconds': round(time.monotonic() - started, 3)}))
        raise SystemExit(0)
    time.sleep(1)
raise SystemExit('bucket list readiness timed out')
