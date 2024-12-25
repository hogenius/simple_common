import datetime
import sqlite3
from enum import Enum

# Enum 정의
class TableType(Enum):
    Msg = "table_msg"
    Check = "table_check"

class SimpleData:
    def __init__(self, db_path='example.db'):
        self.db_path = db_path

    def _connect(self):
        """ 데이터베이스 연결을 관리하는 내부 메서드 """
        return sqlite3.connect(self.db_path, timeout=10)

    def _get_table_name(self, table_type):
        """ Enum에 따라 테이블 이름을 반환하는 메서드 """
        return table_type.value

    def load_strings(self, table_type):
        """ 문자열을 로드하고 삭제하는 메서드 """
        conn = self._connect()
        cursor = conn.cursor()
        string_list = []

        table_name = self._get_table_name(table_type)

        try:
            # 트랜잭션 시작
            conn.execute('BEGIN IMMEDIATE')

            # 테이블이 존재하지 않으면 생성
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    text_value TEXT NOT NULL
                )
            ''')

            # 테이블에서 모든 행을 SELECT
            cursor.execute(f"SELECT id, text_value FROM {table_name}")
            rows = cursor.fetchall()

            # SELECT한 모든 행에 대해 DELETE 수행
            for row in rows:
                cursor.execute(f"DELETE FROM {table_name} WHERE id = ?", (row[0],))
                string_list.append(row[1])  # 문자열을 리스트에 추가

            # 변경 사항을 커밋
            conn.commit()

        except sqlite3.DatabaseError as e:
            print(f"Database error occurred: {e}")
            conn.rollback()  # 오류 발생 시 롤백

        finally:
            # 연결 종료
            cursor.close()
            conn.close()

        return string_list

    def add_string(self, table_type, text_value):
        """ 문자열을 추가하는 메서드 """
        conn = self._connect()
        cursor = conn.cursor()

        table_name = self._get_table_name(table_type)

        try:
            # 트랜잭션 시작
            conn.execute('BEGIN IMMEDIATE')

            # 테이블이 존재하지 않으면 생성
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    text_value TEXT NOT NULL
                )
            ''')

            # 문자열을 테이블에 삽입
            cursor.execute(f"INSERT INTO {table_name} (text_value) VALUES (?)", (text_value,))

            # 변경 사항을 커밋
            conn.commit()

        except sqlite3.DatabaseError as e:
            print(f"Database error occurred: {e}")
            conn.rollback()  # 오류 발생 시 롤백

        finally:
            # 연결 종료
            cursor.close()
            conn.close()

    def _ensure_table_exists(self, conn):
        """ 테이블이 존재하지 않으면 생성하는 메서드 """
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS common_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                value1 TEXT,
                value2 TEXT,
                value3 TEXT,
                value4 TEXT,
                number1 REAL,
                number2 REAL,
                number3 REAL,
                number4 REAL,
                date TEXT NOT NULL
            )
        ''')
        cursor.close()

    def insert_common_data(self, data_type, value1, value2, value3, value4, number1, number2, number3, number4, record_date):
        """ 데이터를 삽입하는 메서드 """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            self._ensure_table_exists(conn)
            date_str = record_date.strftime("%Y-%m-%d %H:%M:%S")

            cursor.execute('''
                INSERT INTO common_data (type, value1, value2, value3, value4, number1, number2, number3, number4, date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (data_type, value1, value2, value3, value4, number1, number2, number3, number4, date_str))
            conn.commit()
            print(f"Inserted record of type: {data_type} with date: {date_str}")

        except sqlite3.DatabaseError as e:
            print(f"Database error occurred: {e}")
            conn.rollback()

        finally:
            cursor.close()
            conn.close()

    def get_common_data(self, data_type, query_date):
        """ 특정 날짜의 데이터를 조회하는 메서드 """
        conn = self._connect()
        cursor = conn.cursor()
        result = []

        try:
            self._ensure_table_exists(conn)
            date_str = query_date.strftime("%Y-%m-%d")

            cursor.execute('''
                SELECT * FROM common_data
                WHERE type = ? AND DATE(date) = ?
            ''', (data_type, date_str))
            result = cursor.fetchall()

        except sqlite3.DatabaseError as e:
            print(f"Database error occurred: {e}")

        finally:
            cursor.close()
            conn.close()

        return result

    def get_common_data_between_dates(self, data_type, start_date, end_date):
        """ 두 날짜 사이의 데이터를 조회하는 메서드 """
        conn = self._connect()
        cursor = conn.cursor()
        result = []

        try:
            self._ensure_table_exists(conn)
            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

            cursor.execute('''
                SELECT * FROM common_data
                WHERE type = ? AND date BETWEEN ? AND ?
            ''', (data_type, start_date_str, end_date_str))
            result = cursor.fetchall()

        except sqlite3.DatabaseError as e:
            print(f"Database error occurred: {e}")

        finally:
            cursor.close()
            conn.close()

        return result

    def update_common_data(self, record_id, value1, value2, value3, value4, number1, number2, number3, number4, record_date):
        """ 특정 ID의 데이터를 업데이트하는 메서드 """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            self._ensure_table_exists(conn)
            date_str = record_date.strftime("%Y-%m-%d %H:%M:%S")

            cursor.execute('''
                UPDATE common_data
                SET value1 = ?, value2 = ?, value3 = ?, value4 = ?,
                    number1 = ?, number2 = ?, number3 = ?, number4 = ?,
                    date = ?
                WHERE id = ?
            ''', (value1, value2, value3, value4, number1, number2, number3, number4, date_str, record_id))
            conn.commit()
            print(f"Updated record with ID: {record_id}")

        except sqlite3.DatabaseError as e:
            print(f"Database error occurred: {e}")
            conn.rollback()

        finally:
            cursor.close()
            conn.close()

    def delete_common_data_by_id(self, record_id):
        """ 특정 ID의 데이터를 삭제하는 메서드 """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            self._ensure_table_exists(conn)

            cursor.execute('''
                DELETE FROM common_data
                WHERE id = ?
            ''', (record_id,))
            deleted_count = cursor.rowcount
            conn.commit()
            print(f"Deleted {deleted_count} record(s) with ID: {record_id}")

        except sqlite3.DatabaseError as e:
            print(f"Database error occurred: {e}")
            conn.rollback()

        finally:
            cursor.close()
            conn.close()

    def delete_common_data(self, days):
        """ 오래된 데이터를 삭제하는 메서드 """
        conn = self._connect()
        cursor = conn.cursor()

        try:
            self._ensure_table_exists(conn)
            now = datetime.datetime.now(datetime.timezone.utc)
            cutoff_date = (now - datetime.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

            cursor.execute('''
                DELETE FROM common_data
                WHERE date < ?
            ''', (cutoff_date,))
            deleted_count = cursor.rowcount
            conn.commit()
            print(f"Deleted {deleted_count} record(s) older than {days} days.")

        except sqlite3.DatabaseError as e:
            print(f"Database error occurred: {e}")
            conn.rollback()

        finally:
            cursor.close()
            conn.close()

# 사용 예제
if __name__ == "__main__":
    # 특정 경로의 데이터베이스 파일을 사용
    db_path = 'example.db'  # 여기에 원하는 데이터베이스 파일 경로를 지정하세요
    simple_data = SimpleData(db_path=db_path)

    # 테이블 A에 문자열 추가
    text_to_insert = "Sample Text for Table A"
    simple_data.add_string(TableType.Msg, text_to_insert)
    print(f"Successfully inserted into Table A: {text_to_insert}")

    # 테이블 B에 문자열 추가
    text_to_insert_b = "Sample Text for Table B"
    simple_data.add_string(TableType.Check, text_to_insert_b)
    print(f"Successfully inserted into Table B: {text_to_insert_b}")

    # 테이블 A에서 문자열 로드 및 삭제
    result_a = simple_data.load_strings(TableType.Msg)
    print(f"Loaded strings from Table A: {result_a}")

    # 테이블 B에서 문자열 로드 및 삭제
    result_b = simple_data.load_strings(TableType.Check)
    print(f"Loaded strings from Table B: {result_b}")

    current_date = datetime.datetime.now(datetime.timezone.utc)

    # 데이터 삽입
    simple_data.insert_common_data("config", "value1", "value2", "value3", "value4", 1, 2.0, 3.0, 4.0, current_date)

    # 데이터 업데이트
    simple_data.update_common_data(1, "updated1", "updated2", "updated3", "updated4", 10.0, 20.0, 30.0, 40.0, current_date)

    # 특정 날짜의 데이터 조회
    records = simple_data.get_common_data("config", current_date)
    print(f"Records: {records}")

    # 두 날짜 사이의 데이터 조회
    start_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    end_date = datetime.datetime.now(datetime.timezone.utc)
    records_between = simple_data.get_common_data_between_dates("config", start_date, end_date)
    print(f"Records between dates: {records_between}")

    # 오래된 데이터 삭제
    simple_data.delete_common_data(10)

    # 특정 ID의 데이터 삭제
    simple_data.delete_common_data_by_id(1)