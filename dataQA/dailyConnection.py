import ConfigParser
import psycopg2


cp = ConfigParser.ConfigParser()
cp.read("redshift.cfg")
conn_str = cp.get('redshift', 'connection_string')
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()


def execute(message):
    cursor.execute(message)
    conn.commit()
