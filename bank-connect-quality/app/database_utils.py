import timeit

from databases import Database
import psycopg2
import psycopg2.extras
from .conf import PORTAL_DB_URL, QUALITY_DB_URL, \
    SELF_HOSTED_CLICKHOUSE_DATABASE, SELF_HOSTED_CLICKHOUSE_HOST, SELF_HOSTED_CLICKHOUSE_PASSWORD, \
    SELF_HOSTED_CLICKHOUSE_PORT, \
    SELF_HOSTED_CLICKHOUSE_USER, QUALITY_HOST, QUALITY_PORT, QUALITY_DB, QUALITY_USER, QUALITY_PASSWORD, PORT, STAGE, \
    BANK_CONNECT_USER, BANK_CONNECT_PASSWORD, BANK_CONNECT_DB_READ_REPLICA_HOST_URL, DATABASE as BANK_CONNECT_DATABASE
import clickhouse_connect
import sentry_sdk
import traceback

from .constants import QUALITY_DATABASE_NAME, PORTAL_DATABASE_NAME


def prepare_clickhouse_client():
    clickhouse_client_obj = None
    try:
        clickhouse_client_obj = clickhouse_connect.get_client(
            host=SELF_HOSTED_CLICKHOUSE_HOST,
            port=SELF_HOSTED_CLICKHOUSE_PORT,
            username=SELF_HOSTED_CLICKHOUSE_USER,
            password=SELF_HOSTED_CLICKHOUSE_PASSWORD,
            database=SELF_HOSTED_CLICKHOUSE_DATABASE,
            secure=False
        )
    except Exception as e:
        sentry_sdk.capture_exception(e)
        print(traceback.format_exc())
    return clickhouse_client_obj

portal_db = Database(PORTAL_DB_URL)
# quality_db = Database(LOCAL_DB_URL)
quality_database = Database(QUALITY_DB_URL)
clickhouse_client = prepare_clickhouse_client()


class DBConnection(object):
    def __init__(self, database_name: str):
        self.connection = None
        self.database_name = database_name

    def get_connection(self, new=False):
        """Creates return new Singleton database connection"""
        connection_string=None

        if self.database_name == PORTAL_DATABASE_NAME:
            connection_string = f"postgres://{BANK_CONNECT_USER}:{BANK_CONNECT_PASSWORD}@{BANK_CONNECT_DB_READ_REPLICA_HOST_URL}:{PORT}/{BANK_CONNECT_DATABASE}"

        if self.database_name == QUALITY_DATABASE_NAME:
            if STAGE.upper() == "PROD":
                connection_string = f"postgres://{QUALITY_USER}:{QUALITY_PASSWORD}@{QUALITY_HOST}:{QUALITY_PORT}/{QUALITY_DB}?sslmode=allow"
            else:
                connection_string = f"postgres://{QUALITY_USER}:{QUALITY_PASSWORD}@{QUALITY_HOST}:{PORT}/{QUALITY_DB}"

        if (new or not self.connection) and connection_string is not None:
            self.connection = psycopg2.connect(connection_string)
        return self.connection

    def execute_query(self, query, values=dict):
        """execute query on singleton db connection"""
        result = None
        connection = self.get_connection()
        try:
            cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        except Exception:
            print(traceback.format_exc())
            connection = self.get_connection(new=True)  # Create new connection
            cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, values)

        if "SELECT" in cursor.statusmessage:
            result = cursor.fetchall()
        else:
            result = True if cursor.rowcount > 0 else False
        connection.commit()
        cursor.close()
        return result
