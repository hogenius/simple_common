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

# 사용 예제
if __name__ == "__main__":
    # 특정 경로의 데이터베이스 파일을 사용
    db_path = 'example.db'  # 여기에 원하는 데이터베이스 파일 경로를 지정하세요
    simple_data = SimpleData(db_path=db_path)

    # 테이블 A에 문자열 추가
    text_to_insert = "Sample Text for Table A"
    simple_data.add_string(text_to_insert, TableType.Msg)
    print(f"Successfully inserted into Table A: {text_to_insert}")

    # 테이블 B에 문자열 추가
    text_to_insert_b = "Sample Text for Table B"
    simple_data.add_string(text_to_insert_b, TableType.Check)
    print(f"Successfully inserted into Table B: {text_to_insert_b}")

    # 테이블 A에서 문자열 로드 및 삭제
    result_a = simple_data.load_strings(TableType.Msg)
    print(f"Loaded strings from Table A: {result_a}")

    # 테이블 B에서 문자열 로드 및 삭제
    result_b = simple_data.load_strings(TableType.Check)
    print(f"Loaded strings from Table B: {result_b}")

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

# 사용 예제
if __name__ == "__main__":
    # 특정 경로의 데이터베이스 파일을 사용
    db_path = 'example.db'  # 여기에 원하는 데이터베이스 파일 경로를 지정하세요
    simple_data = SimpleData(db_path=db_path)

    # 테이블 A에 문자열 추가
    text_to_insert = "Sample Text for Table A"
    simple_data.add_string(text_to_insert, TableType.Msg)
    print(f"Successfully inserted into Table A: {text_to_insert}")

    # 테이블 B에 문자열 추가
    text_to_insert_b = "Sample Text for Table B"
    simple_data.add_string(text_to_insert_b, TableType.Check)
    print(f"Successfully inserted into Table B: {text_to_insert_b}")

    # 테이블 A에서 문자열 로드 및 삭제
    result_a = simple_data.load_strings(TableType.Msg)
    print(f"Loaded strings from Table A: {result_a}")

    # 테이블 B에서 문자열 로드 및 삭제
    result_b = simple_data.load_strings(TableType.Check)
    print(f"Loaded strings from Table B: {result_b}")
