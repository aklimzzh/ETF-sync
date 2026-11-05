from __future__ import annotations

import hmac
import os
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg2
from flask import Flask, jsonify, request

ENV_FILE = Path('/usr/home/aklim/.config/etf-api.env')
for line in ENV_FILE.read_text(encoding='utf-8').splitlines():
    line = line.strip()
    if line and not line.startswith('#') and '=' in line:
        key, value = line.split('=', 1)
        os.environ[key.strip()] = value.strip()

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024
DB_CONFIG = {
    'host': os.environ['DB_HOST'], 'port': int(os.environ.get('DB_PORT', '5432')),
    'dbname': os.environ['DB_NAME'], 'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'], 'connect_timeout': 10,
}
API_TOKEN = os.environ['API_TOKEN']


def connection():
    return psycopg2.connect(**DB_CONFIG)


def authorized() -> bool:
    return hmac.compare_digest(request.headers.get('Authorization', ''), f'Bearer {API_TOKEN}')


@app.get('/')
def index():
    return jsonify(service='ETF API', status='running')


@app.get('/api/v1/health')
def health():
    try:
        with connection() as conn, conn.cursor() as cur:
            cur.execute('''SELECT current_database(), current_user, NOW(),
                (SELECT COUNT(*) FROM etf_master),
                (SELECT COUNT(*) FROM etf_share_daily),
                (SELECT COUNT(*) FROM import_log)''')
            db, user, now, master, daily, logs = cur.fetchone()
        return jsonify(success=True, database=db, user=user, server_time=now.isoformat(),
                       tables={'etf_master': master, 'etf_share_daily': daily, 'import_log': logs})
    except Exception:
        app.logger.exception('Health failed')
        return jsonify(success=False, error='Database connection failed'), 500


@app.post('/api/v1/test')
def write_test():
    if not authorized():
        return jsonify(success=False, error='Unauthorized'), 401
    body = request.get_json(silent=True) or {}
    source = str(body.get('source', 'github-api-test'))[:50]
    message = str(body.get('message', 'API write test'))[:500]
    conn = connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO connection_test (source, message)
                VALUES (%s, %s) RETURNING id, source, message, created_at""",
                (source, message))
            inserted = cur.fetchone()
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("""SELECT id, source, message, created_at
                FROM connection_test WHERE id=%s""", (inserted[0],))
            verified = cur.fetchone()
        if verified is None:
            raise RuntimeError('Inserted record could not be read back')
        return jsonify(success=True, committed=True, verified=True,
                       record={'id': verified[0], 'source': verified[1],
                               'message': verified[2],
                               'created_at': verified[3].isoformat()}), 201
    except Exception:
        conn.rollback()
        app.logger.exception('Write test failed')
        return jsonify(success=False, error='Database write failed'), 500
    finally:
        conn.close()


@app.post('/api/v1/import')
def import_records():
    if not authorized():
        return jsonify(success=False, error='Unauthorized'), 401
    body = request.get_json(silent=True) or {}
    exchange = str(body.get('exchange', '')).upper()
    if exchange not in {'SSE', 'SZSE'}:
        return jsonify(success=False, error='Invalid exchange'), 400
    try:
        trade_date = date.fromisoformat(str(body['trade_date']))
    except Exception:
        return jsonify(success=False, error='Invalid trade_date'), 400
    records = body.get('records')
    if not isinstance(records, list) or not records or len(records) > 500:
        return jsonify(success=False, error='records must contain 1-500 items'), 400
    finalize = bool(body.get('finalize'))
    conn = connection()
    try:
        with conn.cursor() as cur:
            cur.execute('''INSERT INTO import_log
                (trade_date, exchange, status, github_run_id, github_commit)
                VALUES (%s,%s,'STARTED',%s,%s)
                ON CONFLICT (trade_date, exchange) DO UPDATE SET
                status='STARTED', started_at=NOW(), finished_at=NULL,
                github_run_id=EXCLUDED.github_run_id,
                github_commit=EXCLUDED.github_commit, error_message=NULL''',
                (trade_date, exchange, body.get('github_run_id'), body.get('github_commit')))
            for row in records:
                if row.get('exchange') != exchange or row.get('trade_date') != trade_date.isoformat():
                    raise ValueError('Record scope does not match request')
                share = Decimal(str(row['total_share']))
                if share < 0:
                    raise ValueError('Negative total_share')
                cur.execute('''INSERT INTO etf_master
                    (exchange,fund_code,fund_name,etf_type,first_seen_date,last_seen_date)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (exchange,fund_code) DO UPDATE SET
                    fund_name=EXCLUDED.fund_name,
                    etf_type=COALESCE(EXCLUDED.etf_type,etf_master.etf_type),
                    last_seen_date=GREATEST(etf_master.last_seen_date,EXCLUDED.last_seen_date),
                    active=TRUE, updated_at=NOW()
                    RETURNING id''',
                    (exchange, str(row['fund_code']), str(row['fund_name']), row.get('etf_type'), trade_date, trade_date))
                etf_id = cur.fetchone()[0]
                cur.execute('''INSERT INTO etf_share_daily
                    (etf_id,trade_date,total_share,source)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (etf_id,trade_date) DO UPDATE SET
                    total_share=EXCLUDED.total_share, fetched_at=NOW(), source=EXCLUDED.source''',
                    (etf_id, trade_date, share, str(row['source'])))
            if finalize:
                cur.execute('''SELECT COUNT(*) FROM etf_share_daily s
                    JOIN etf_master e ON e.id=s.etf_id
                    WHERE s.trade_date=%s AND e.exchange=%s''', (trade_date, exchange))
                count = cur.fetchone()[0]
                cur.execute('''UPDATE import_log SET status='SUCCESS', finished_at=NOW(), record_count=%s
                    WHERE trade_date=%s AND exchange=%s''', (count, trade_date, exchange))
            else:
                count = None
        conn.commit()
        return jsonify(success=True, committed=True, exchange=exchange,
                       trade_date=trade_date.isoformat(), batch_no=body.get('batch_no'),
                       finalize=finalize, record_count=count), 201
    except (KeyError, ValueError, InvalidOperation) as exc:
        conn.rollback()
        app.logger.exception('Import validation failed')
        return jsonify(success=False, error=str(exc)), 400
    except Exception:
        conn.rollback()
        app.logger.exception('Import failed')
        return jsonify(success=False, error='Database import failed'), 500
    finally:
        conn.close()


if __name__ == '__main__':
    app.run()
