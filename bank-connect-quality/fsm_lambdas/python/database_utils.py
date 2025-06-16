import os
import psycopg2
import psycopg2.extras
import psycopg2.pool
from sentry_sdk import capture_exception
from threading import Lock

#TODO: Move environment variables in python.configs file.
# Environment variables for RDS Connection
BANK_CONNECT_RDS_HOST_URL = os.getenv('BANK_CONNECT_RDS_HOST_URL')
BANK_CONNECT_RDS_PORT = os.getenv('BANK_CONNECT_RDS_PORT')
BANK_CONNECT_RDS_DBNAME = os.getenv('BANK_CONNECT_RDS_DBNAME')
BANK_CONNECT_RDS_USER = os.getenv('BANK_CONNECT_RDS_USER')
BANK_CONNECT_RDS_PASSWORD = os.getenv('BANK_CONNECT_RDS_PASSWORD')

class DBConnection:
    _pool = None
    _lock = Lock()  # To ensure thread safety

    def __new__(cls, *args, **kwargs):
        # Implement Singleton behavior
        if not cls._pool:
            with cls._lock:
                if not cls._pool:
                    cls._pool = super(DBConnection, cls).__new__(cls, *args, **kwargs)._get_connection_pool()
        return super(DBConnection, cls).__new__(cls, *args, **kwargs)

    def _get_connection_pool(self):
        # Creates or returns the Singleton database connection
        print("Creating a new RDS pool connection")
        self._pool = psycopg2.pool.SimpleConnectionPool(
            1, 2,
            host=BANK_CONNECT_RDS_HOST_URL,
            port=BANK_CONNECT_RDS_PORT,
            database=BANK_CONNECT_RDS_DBNAME,
            user=BANK_CONNECT_RDS_USER,
            password=BANK_CONNECT_RDS_PASSWORD
        )
        return self._pool

    def execute_query(self, query, values=dict):
        # Execute query on the Singleton DB connection
        result = None
        connection = self._pool.getconn()
        try:
            cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        except Exception as e:
            capture_exception(e)
            self._pool = self._get_connection_pool()
            connection = self._pool.getconn()
            cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(query, values)
            result = [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            capture_exception(e)
            connection.rollback()
        finally:
            self._pool.putconn(connection)
            cursor.close()
        return result

def prepare_identity_rds_warehouse_data(entity_id, statement_id):
    required_cols = [
        "session_from_date", "session_to_date", "attempt_type", "statement_status", "statement_created_at", "transaction_count", "is_ocr_extracted", "is_multi_account_statement",
        "child_statement_list", "parent_statement_id", "fraud_list", "is_extracted_by_perfios", "pdf_hash", "logo_hash", "is_extracted", "is_complete", "is_processing_requested"
    ]
    identity_rds_data = {}
    for col in required_cols:
        identity_rds_data[col] = None
    bank_connect_session_data = DBConnection().execute_query(
        query="""
            SELECT
                from_date as session_from_date,
                to_date as session_to_date
            FROM
                bank_connect_session
            WHERE
                session_id = %(session_id)s
        """, 
        values={
            "session_id": entity_id
        }
    )
    bank_connect_statement_data = DBConnection().execute_query(
        query="""
            SELECT
                attempt_type,
                statement_status,
                created_at as statement_created_at,
                transaction_count,
                is_ocr_extracted,
                is_multi_account_statement,
                child_statement_list,
                parent_statement_id,
                fraud_list,
                is_extracted_by_perfios,
                pdf_hash,
                logo_hash,
                is_extracted,
                is_complete
            FROM
                bank_connect_statement
            WHERE
                statement_id = %(statement_id)s""",
        values={
            "statement_id": statement_id
        }
    )
    bank_connect_entity_data = DBConnection().execute_query(
        query="""
            SELECT
                is_processing_requested
            FROM
                bank_connect_entity
            WHERE
                entity_id = %(entity_id)s
        """,
        values={
            "entity_id": entity_id
        }
    )
    for dict_obj in bank_connect_session_data+bank_connect_statement_data+bank_connect_entity_data:
        for key, value in dict_obj.items():
            identity_rds_data[key] = value
    return identity_rds_data
