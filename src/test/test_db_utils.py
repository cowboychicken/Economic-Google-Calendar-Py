# test_db.py
from src.utils.db_utils import DatabaseConnection





def test_database_conection_success():

    try:
        with DatabaseConnection() as conn:
            assert conn is not None

            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                result = cur.fetchone()
                assert result==(1,)
    except Exception as e:
        assert False, f"Database connection failed: {e}"

