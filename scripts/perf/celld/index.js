// Comparison-only application. Framework code and durability gates are unchanged.
export class Orders {
  constructor(state) {
    this.state = state;
    this.sql = state.storage.sql;
    this.sql.exec('CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, total_cents INTEGER NOT NULL, value TEXT NOT NULL)');
    this.sql.exec('CREATE TABLE IF NOT EXISTS commands (request_id TEXT PRIMARY KEY, input TEXT NOT NULL, response TEXT NOT NULL)');
    this.sql.exec('CREATE TABLE IF NOT EXISTS metadata (id INTEGER PRIMARY KEY, incarnation TEXT NOT NULL, sequence INTEGER NOT NULL)');
    this.sql.exec('INSERT OR IGNORE INTO metadata VALUES (1, ?, 0)', crypto.randomUUID().replaceAll('-', ''));
  }
  receipt() {
    const row = this.sql.exec('SELECT incarnation, sequence FROM metadata WHERE id = 1').one();
    return {cell:this.state.id.toString(), incarnation:row.incarnation, commit_sequence:row.sequence};
  }
  read(id) {
    const rows = this.sql.exec('SELECT id, total_cents, value FROM orders WHERE id = ?', id).toArray();
    return {output:rows[0] ?? null, receipt:this.receipt()};
  }
  async fetch(request) {
    if (request.method === 'GET') {
      const id = Number(new URL(request.url).pathname.split('/').pop());
      return Response.json(this.read(id));
    }
    const body = await request.json();
    const input = {id:body.id,total_cents:body.total_cents,value:body.value ?? 'p'.repeat(96)};
    const canonical = JSON.stringify(input);
    if (!Number.isSafeInteger(input.id) || !Number.isSafeInteger(input.total_cents) || typeof input.value !== 'string') {
      return Response.json({error:'invalid input'}, {status:400});
    }
    let result;
    let status = 201;
    this.state.storage.transactionSync(() => {
      const saved = this.sql.exec('SELECT input, response FROM commands WHERE request_id = ?', body.request_id).toArray()[0];
      if (saved) {
        if (saved.input !== canonical) {status=409;result={error:'request identity conflict'};return;}
        result=JSON.parse(saved.response);return;
      }
      if (body.expires_at_ms <= Date.now() || body.issued_at_ms > Date.now()) {
        status=400;result={error:'request identity expired or not yet valid'};return;
      }
      this.sql.exec('INSERT INTO orders (id, total_cents, value) VALUES (?, ?, ?)', input.id,input.total_cents,input.value);
      this.sql.exec('UPDATE metadata SET sequence = sequence + 1 WHERE id = 1');
      result=this.read(input.id);
      this.sql.exec('INSERT INTO commands VALUES (?, ?, ?)', body.request_id,canonical,JSON.stringify(result));
    });
    return Response.json(result, {status});
  }
}
export default {
  async fetch(request, env) {
    let id;
    if (request.method === 'POST') id=(await request.clone().json()).id;
    else id=Number(new URL(request.url).pathname.split('/').pop());
    const count=Number(env.CELLS);
    const shard=((id % count)+count)%count;
    return env.ORDERS.get(env.ORDERS.idFromName(String(shard))).fetch(request);
  }
};
