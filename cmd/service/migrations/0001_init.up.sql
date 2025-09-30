CREATE TABLE IF NOT EXISTS orders (
  order_uid        TEXT PRIMARY KEY,
  track_number     TEXT NOT NULL,
  entry            TEXT NOT NULL,
  locale           TEXT,
  internal_signature TEXT,
  customer_id      TEXT,
  delivery_service TEXT,
  shardkey         TEXT,
  sm_id            INTEGER,
  date_created     TIMESTAMPTZ NOT NULL,
  oof_shard        TEXT
);

CREATE TABLE IF NOT EXISTS deliveries (
  order_uid  TEXT PRIMARY KEY REFERENCES orders(order_uid) ON DELETE CASCADE,
  name       TEXT,
  phone      TEXT,
  zip        TEXT,
  city       TEXT,
  address    TEXT,
  region     TEXT,
  email      TEXT
);

CREATE TABLE IF NOT EXISTS payments (
  order_uid     TEXT PRIMARY KEY REFERENCES orders(order_uid) ON DELETE CASCADE,
  transaction   TEXT NOT NULL,
  request_id    TEXT,
  currency      TEXT,
  provider      TEXT,
  amount        INTEGER,
  payment_dt    TIMESTAMPTZ,
  bank          TEXT,
  delivery_cost INTEGER,
  goods_total   INTEGER,
  custom_fee    INTEGER
);

CREATE TABLE IF NOT EXISTS items (
  order_uid   TEXT REFERENCES orders(order_uid) ON DELETE CASCADE,
  chrt_id     BIGINT,
  track_number TEXT,
  price       INTEGER,
  rid         TEXT,
  name        TEXT,
  sale        INTEGER,
  size        TEXT,
  total_price INTEGER,
  nm_id       BIGINT,
  brand       TEXT,
  status      INTEGER,
  PRIMARY KEY(order_uid, chrt_id)
); 