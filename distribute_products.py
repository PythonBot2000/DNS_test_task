import sys
import configparser
import psycopg2

def load_config(path='config.ini'):
	cfg = configparser.ConfigParser()
	if not cfg.read(path):
		print(f"Cannot read config file: {path}", file=sys.stderr)
		sys.exit(1)
	return cfg

def distribute():
	cfg = load_config()
	db = cfg['database']
	conn = psycopg2.connect(
		dbname=db.get('name'), user=db.get('user'), password=db.get('password'),
		host=db.get('host', 'localhost'), port=db.get('port', '5432')
	)
	cur = conn.cursor()

	cur.execute("DROP TABLE IF EXISTS shipments;")
	cur.execute(
		"""
		CREATE TABLE shipments (
			branch_id    UUID NOT NULL,
			product_id   UUID NOT NULL,
			shipment_qty INT  NOT NULL
		);
		"""
	)
	conn.commit()

	distribution_sql = """
	WITH
	available AS (
		SELECT bp.product_id, bp.branch_id,
			GREATEST(0, n.need - (bp.stock + bp.transit - bp.reserve)) AS deficit,
			s.priority
		FROM branch_products bp
		JOIN needs n ON bp.branch_id = n.branch_id AND bp.product_id = n.product_id
		JOIN stores s ON bp.branch_id = s.branch_id
	),
	rc AS (
		SELECT product_id, SUM(stock)::BIGINT AS total_stock
		FROM rc_products GROUP BY product_id
	),
	sum_weights AS (
		SELECT product_id, SUM(deficit * priority) AS total_weight
		FROM available GROUP BY product_id
	),
	first_alloc AS (
		SELECT a.product_id, a.branch_id, a.deficit, a.priority,
			rc.total_stock, sw.total_weight
		FROM available a
		JOIN rc ON rc.product_id = a.product_id
		JOIN sum_weights sw ON sw.product_id = a.product_id
	),
	first_pass AS (
		SELECT
		product_id,
		branch_id,
		deficit,
		priority,
		LEAST(
			COALESCE(
			FLOOR(
				total_stock * (deficit * priority)::numeric
				/ NULLIF(total_weight, 0)
			)::int,
			0
			),
			deficit
		) AS shipped
		FROM first_alloc
	),
	remaining AS (
		SELECT fp.product_id,
			rc.total_stock - SUM(fp.shipped) AS remaining
		FROM first_pass fp
		JOIN rc ON rc.product_id = fp.product_id
		GROUP BY fp.product_id, rc.total_stock
	),
	pass1 AS (
		SELECT fp.product_id, fp.branch_id,
			fp.deficit - fp.shipped AS deficit,
			fp.priority,
			fp.shipped,
			rem.remaining
		FROM first_pass fp
		JOIN remaining rem ON rem.product_id = fp.product_id
	),
	second_pass AS (
		SELECT
		branch_id,
		product_id,
		shipped +
		CASE WHEN ROW_NUMBER() OVER (
			PARTITION BY product_id ORDER BY priority DESC
			) <= remaining THEN 1 ELSE 0 END
		AS shipment_qty
		FROM pass1
	)
	INSERT INTO shipments(branch_id, product_id, shipment_qty)
	SELECT branch_id, product_id, shipment_qty
	FROM second_pass
	WHERE shipment_qty > 0;
	"""

	cur.execute(distribution_sql)
	conn.commit()

	adjustment_sql = """
	WITH
	branch_sums AS (
		SELECT branch_id, SUM(shipment_qty) AS total_shipment
		FROM shipments GROUP BY branch_id
	),
	to_zero AS (
		SELECT bs.branch_id
		FROM branch_sums bs
		JOIN stores s ON bs.branch_id = s.branch_id
		WHERE bs.total_shipment < s.min_shipment
	),
	zeroed AS (
		DELETE FROM shipments sh
		USING to_zero tz
		WHERE sh.branch_id = tz.branch_id
		RETURNING sh.product_id, sh.shipment_qty
	),
	leftovers AS (
		SELECT product_id, SUM(shipment_qty) AS leftover
		FROM zeroed GROUP BY product_id
	),
	avail AS (
		SELECT bp.branch_id, bp.product_id, s.priority
		FROM branch_products bp
		JOIN stores s ON bp.branch_id = s.branch_id
		JOIN needs n ON bp.branch_id = n.branch_id AND bp.product_id = n.product_id
		WHERE GREATEST(0, n.need - (bp.stock + bp.transit - bp.reserve)) > 0
	),
	alloc AS (
		SELECT a.branch_id, l.product_id,
			CASE WHEN ROW_NUMBER() OVER (
				PARTITION BY l.product_id ORDER BY a.priority DESC
			) <= l.leftover THEN 1 ELSE 0 END AS add_qty
		FROM leftovers l
		JOIN avail a ON a.product_id = l.product_id
	)
	INSERT INTO shipments(branch_id, product_id, shipment_qty)
	SELECT branch_id, product_id, add_qty
	FROM alloc
	WHERE add_qty > 0;
	"""
	cur.execute(adjustment_sql)
	conn.commit()

	cur.close()
	conn.close()
	print("Distribution complete, results written to 'shipments'.")

if __name__ == '__main__':
	distribute()
