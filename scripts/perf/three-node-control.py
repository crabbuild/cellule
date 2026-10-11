"""HTTP seed and streaming warm/cold audit for the three-writer example."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
from pathlib import Path
import threading

spec = importlib.util.spec_from_file_location(
    'http_bench', Path('/source/scripts/bench-axum-rustfs.py'))
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class Connections:
    def __init__(self, addresses, cells):
        self.addresses, self.cells = addresses, cells
        self.local = threading.local()
        self.all = []
        self.lock = threading.Lock()

    def for_id(self, order_id):
        node = (order_id % self.cells) % len(self.addresses)
        if not hasattr(self.local, 'connections'):
            self.local.connections = {}
        if node not in self.local.connections:
            connection = bench.http.client.HTTPConnection(self.addresses[node], timeout=60)
            self.local.connections[node] = connection
            with self.lock:
                self.all.append(connection)
        return self.local.connections[node]

    def close(self):
        for connection in self.all:
            connection.close()


def seed(config, connections):
    def create(order_id):
        connection = connections.for_id(order_id)
        initial = bench.request(connection, 'GET', f'/orders/{order_id}')
        bench.require(initial['status'] == 200 and initial['body']['output'] is None
                      and initial['body']['receipt']['commit_sequence'] == 0,
                      'seed requires fresh sequence-zero Cells')
        envelope = bench.identity(order_id)
        reply = bench.request(connection, 'POST', '/orders', envelope)
        bench.verify_order(reply, envelope, initial['body']['receipt'], 201)
        bench.require(reply['body']['receipt']['commit_sequence'] == 1,
                      'seed did not create the first mutation')
        return dict(request=envelope, response=reply['body'], error=None)

    directory = Path(config['directory'])
    with ThreadPoolExecutor(max_workers=32) as pool, (directory / 'seed.jsonl').open('x') as journal:
        observations = []
        for record in pool.map(create, range(config['cells'])):
            journal.write(json.dumps(record) + '\n')
            observations.append(record['response'])
    bench.require(len({item['receipt']['cell'] for item in observations}) == config['cells'],
                  'nodes did not cover distinct global Cell IDs')
    (directory / 'seeds.json').write_text(json.dumps(observations) + '\n')
    return dict(seeded=len(observations), distinct_cells=len(observations))


def records(config):
    directory = Path(config['directory'])
    paths = [directory / 'seed.jsonl', *sorted(directory.glob('point-*/client-*.jsonl'))]
    if (directory / 'recovered-writes.jsonl').exists():
        paths.append(directory / 'recovered-writes.jsonl')
    for path in paths:
        with path.open() as source:
            for line in source:
                record = json.loads(line)
                if record['request'] is not None and record['error'] is None:
                    yield record


def write_recovered(config, connections):
    def write(shard):
        order_id = config['cells'] + config['write_offset'] + shard
        connection = connections.for_id(order_id)
        initial = bench.request(connection, 'GET', f'/orders/{order_id}')
        bench.require(initial['status'] == 200 and initial['body']['output'] is None,
                      'post-recovery order identity already exists')
        envelope = bench.identity(order_id)
        reply = bench.request(connection, 'POST', '/orders', envelope)
        bench.verify_order(reply, envelope, initial['body']['receipt'], 201)
        bench.require(reply['body']['receipt']['commit_sequence'] == initial['body']['receipt']['commit_sequence'] + 1,
                      'post-recovery write did not advance exactly one command')
        return dict(request=envelope, response=reply['body'], error=None)

    cells = set()
    directory = Path(config['directory'])
    with ThreadPoolExecutor(max_workers=32) as pool, (directory / 'recovered-writes.jsonl').open('x') as journal:
        for record in pool.map(write, range(config['cells'])):
            journal.write(json.dumps(record) + '\n')
            cells.add(record['response']['receipt']['cell'])
    bench.require(len(cells) == config['cells'], 'post-recovery writes did not cover every Cell')
    return dict(acknowledged_writes=len(cells), distinct_cells=len(cells))


def audit(config, connections):
    def verify(record):
        envelope, expected = record['request'], record['response']
        connection = connections.for_id(envelope['id'])
        reply = bench.request(connection, 'GET', f'/orders/{envelope["id"]}')
        bench.verify_order(reply, envelope, expected['receipt'])
        retry = bench.request(connection, 'POST', '/orders', envelope)
        bench.verify_order(retry, envelope, expected['receipt'], 201)
        bench.require(retry['body'] == expected, 'retry changed its exact durable output/receipt')
        return 1

    count = 0
    # Keep at most 256 full request/response records in flight, even for long runs.
    with ThreadPoolExecutor(max_workers=32) as pool:
        batch = []
        for record in records(config):
            batch.append(record)
            if len(batch) == 256:
                count += sum(pool.map(verify, batch))
                batch.clear()
        count += sum(pool.map(verify, batch))
    bench.require(count == config['expected_writes'], 'audit ACK count differs from the driver journals')
    return dict(acknowledged_rows_checked=count, exact_retries_checked=count, errors=0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['seed', 'audit', 'write-recovered'])
    parser.add_argument('config', type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    connections = Connections(config['addresses'], config['cells'])
    try:
        operation = {'seed': seed, 'audit': audit, 'write-recovered': write_recovered}[args.operation]
        result = operation(config, connections)
        print(json.dumps(result))
    finally:
        connections.close()
