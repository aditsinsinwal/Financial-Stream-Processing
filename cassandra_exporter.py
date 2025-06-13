
from flask import Flask, Response
from prometheus_client import Gauge, generate_latest, CollectorRegistry
from cassandra.cluster import Cluster
import os

app = Flask(__name__)
CASS_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = "market_data"

cluster = Cluster([CASS_HOST])
session = cluster.connect(KEYSPACE)

registry = CollectorRegistry()
quotes_count = Gauge(
    'cassandra_quotes_total',
    'Total number of raw quote rows',
    registry=registry
)
avg_count = Gauge(
    'cassandra_quotes_1min_avg_total',
    'Total number of 1min-avg rows',
    registry=registry
)

def update_metrics():
    row = session.execute("SELECT count(*) FROM quotes;").one()
    quotes_count.set(row.count if row else 0)

    row2 = session.execute("SELECT count(*) FROM quotes_1min_avg;").one()
    avg_count.set(row2.count if row2 else 0)

@app.route('/metrics')
def metrics():
    update_metrics()
    return Response(generate_latest(registry), mimetype='text/plain')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=9100)
