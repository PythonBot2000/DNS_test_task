import os
import sys
import configparser
import math
import random
import psycopg2
from psycopg2.extras import execute_values, DictCursor
from tqdm import tqdm

def load_config(path='config.ini'):
	cfg = configparser.ConfigParser()
	if not cfg.read(path):
		print(f"Cannot read config file: {path}", file=sys.stderr)
		sys.exit(1)
	return cfg

def create_tables(cur):
	cur.execute("""
	DROP TABLE IF EXISTS needs, stores, logdays, products, branch_products, rc_products;

	CREATE TABLE rc_products (
		product_id UUID    NOT NULL,
		branch_id  UUID    NOT NULL,
		stock      NUMERIC NOT NULL,
		reserve    NUMERIC NOT NULL,
		transit    NUMERIC NOT NULL,
		PRIMARY KEY(product_id, branch_id)
	);

	CREATE TABLE branch_products (
		product_id UUID    NOT NULL,
		branch_id  UUID    NOT NULL,
		stock      NUMERIC NOT NULL,
		reserve    NUMERIC NOT NULL,
		transit    NUMERIC NOT NULL,
		PRIMARY KEY(product_id, branch_id)
	);

	CREATE TABLE products (
		product_id  UUID PRIMARY KEY,
		category_id UUID NOT NULL
	);

	CREATE TABLE logdays (
		branch_id   UUID NOT NULL,
		category_id UUID NOT NULL,
		logdays     INT  NOT NULL,
		PRIMARY KEY(branch_id, category_id)
	);

	CREATE TABLE stores (
		branch_id    UUID PRIMARY KEY,
		priority     INT  NOT NULL,
		min_shipment INT  NOT NULL DEFAULT 1
	);

	CREATE TABLE needs (
		branch_id  UUID NOT NULL,
		product_id UUID NOT NULL,
		need       INT  NOT NULL,
		PRIMARY KEY(branch_id, product_id)
	);
	""")

def copy_csv(cur, table, csv_path):
	with open(csv_path, 'r', encoding='cp1251') as f:
		next(f)  # skip header
		cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER", f)

def populate_products(cur, conn, prod_csv):
	cur.execute("CREATE TEMP TABLE tmp_products (product_id UUID, category_id UUID);")
	conn.commit()
	with open(prod_csv, 'r', encoding='cp1251') as f:
		next(f)
		cur.copy_expert(
			"COPY tmp_products(product_id, category_id) FROM STDIN WITH CSV HEADER",
			f
		)
	conn.commit()
	cur.execute("""
	INSERT INTO products(product_id, category_id)
	SELECT p.product_id, p.category_id
	FROM tmp_products p
	WHERE EXISTS(SELECT 1 FROM rc_products  rp WHERE rp.product_id = p.product_id)
		OR EXISTS(SELECT 1 FROM branch_products bp WHERE bp.product_id = p.product_id)
	ON CONFLICT DO NOTHING;
	""")
	conn.commit()
	cur.execute("DROP TABLE tmp_products;")
	conn.commit()

def populate_auxiliary(cur, conn):
	cur.execute("""
	INSERT INTO logdays(branch_id, category_id, logdays)
	SELECT b.branch_id, c.category_id, 7
	FROM (SELECT DISTINCT branch_id FROM branch_products) b
	CROSS JOIN (SELECT DISTINCT category_id FROM products) c
	ON CONFLICT DO NOTHING;
	""")
	cur.execute("""
	INSERT INTO stores(branch_id, priority, min_shipment)
	SELECT DISTINCT bp.branch_id,
			(FLOOR(random()*5)+1)::int,
			(FLOOR(random()*20)+1)::int
	FROM branch_products bp
	ON CONFLICT DO NOTHING;
	""")
	conn.commit()

def populate_needs(conn):
	with conn.cursor(cursor_factory=DictCursor) as cur:
		cur.execute("SELECT product_id, category_id FROM products;")
		prod2cat = {r['product_id']: r['category_id'] for r in cur}
		cur.execute("SELECT branch_id, category_id, logdays FROM logdays;")
		log_map = {(r['branch_id'], r['category_id']): r['logdays'] for r in cur}
		cur.execute("SELECT branch_id, min_shipment FROM stores;")
		min_ship_map = {r[0]: r[1] for r in cur.fetchall()}

	read_cur = conn.cursor(name='bp_cur', cursor_factory=DictCursor, withhold=True)
	read_cur.itersize = 10000
	read_cur.execute("SELECT branch_id, product_id, stock FROM branch_products;")
	write_cur = conn.cursor()
	batch = []

	for row in tqdm(read_cur, desc='Processing needs'):
		b, p, stock = row['branch_id'], row['product_id'], float(row['stock'])
		ld = log_map.get((b, prod2cat.get(p)), 7)
		need_calc = math.floor(random.random() * max(150.0, stock) * ld) + 1
		need_val = max(need_calc, min_ship_map.get(b, 1))
		batch.append((b, p, need_val))

		if len(batch) >= 10000:
			execute_values(
				write_cur,
				"INSERT INTO needs(branch_id, product_id, need) VALUES %s ON CONFLICT DO NOTHING",
				batch
			)
			conn.commit()
			batch.clear()

	if batch:
		execute_values(
			write_cur,
			"INSERT INTO needs(branch_id, product_id, need) VALUES %s ON CONFLICT DO NOTHING",
			batch
		)
		conn.commit()

	read_cur.close()
	write_cur.close()

def main():
	cfg = load_config()
	db = cfg['database']
	files = cfg['csv_files']
	base = os.path.dirname(os.path.abspath(__file__))
	rc_csv   = os.path.join(base, files['rc_csv'])
	bp_csv   = os.path.join(base, files['bp_csv'])
	prod_csv = os.path.join(base, files['prod_csv'])

	conn = psycopg2.connect(
		dbname=db['name'],
		user=db['user'],
		password=db['password'],
		host=db.get('host', 'localhost'),
		port=db.get('port', '5432')
	)
	cur = conn.cursor()

	steps = [
		('Creating tables',            lambda: create_tables(cur)),
		('Loading rc_products CSV',    lambda: copy_csv(cur, 'rc_products', rc_csv)   or conn.commit()),
		('Loading branch_products CSV',lambda: copy_csv(cur, 'branch_products', bp_csv) or conn.commit()),
		('Populating products table',  lambda: populate_products(cur, conn, prod_csv)),
		('Populating logdays and stores', lambda: populate_auxiliary(cur, conn)),
		('Populating needs',           lambda: populate_needs(conn))
	]

	overall = tqdm(total=len(steps), desc='Overall progress')
	for desc, func in steps:
		overall.set_description(desc)
		func()
		overall.update(1)
	overall.close()

	cur.close()
	conn.close()
	print('Database population complete.')

if __name__ == '__main__':
	main()
