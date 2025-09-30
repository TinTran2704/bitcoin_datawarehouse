# Standard imports
import fnmatch
import re
import os


# Library imports
import pandas as pd
import psycopg2
from datetime import datetime

# Local imports
# from data_integration.arguments import SELECT
# from data_integration.utils.utils import cleanse_database_object_name



def get_tables_to_sync():
    # Kết nối đến database (thay bằng thông tin thực tế của bạn)
    conn = psycopg2.connect(
        dbname=os.environ.get('PG_DATA_WAREHOUSE_DATABASE'),
        user=os.environ.get('PG_DATA_WAREHOUSE_DATABASE'),
        password=os.environ.get('PG_DATA_WAREHOUSE_DATABASE_PASSWORD'),
        host=os.environ.get('PG_DATA_WAREHOUSE_HOST'),
        port=os.environ.get('PG_DATA_WAREHOUSE_PORT')
    )

    try:
        # Tạo cursor
        cur = conn.cursor()

        # Truy vấn dữ liệu từ bảng etl.etl_job
        # Giả sử JOB_NAME là tên bảng, QUERY_ID là id, TARGET_TABLE là thông tin bổ sung
        cur.execute("""
            SELECT JOB_NAME, QUERY_ID, TARGET_TABLE, P_KEY
            FROM etl.etl_job
            WHERE active = 1
        """)

        # Lấy tất cả các hàng
        rows = cur.fetchall()

        # Chuyển đổi thành danh sách từ điển để khớp với cấu trúc YAML trước đây
        tables_to_sync = [
            {"name": row[0], "id": row[1], "target_table": row[2], "p_key": row[3]}
            for row in rows
        ]

        # Chuyển thành DataFrame (tùy chọn, giữ nguyên logic cũ)
        tables_to_sync_df = pd.DataFrame(tables_to_sync)

        # Đóng cursor
        cur.close()

        return tables_to_sync_df

    except psycopg2.Error as e:
        print(f"Lỗi khi truy vấn database: {e}")
        return None

    finally:
        # Đóng kết nối
        conn.close()

def start_job(job_name):
    # Kết nối đến database (thay bằng thông tin thực tế của bạn)
    conn = psycopg2.connect(
        dbname=os.environ.get('PG_DATA_WAREHOUSE_DATABASE'),
        user=os.environ.get('PG_DATA_WAREHOUSE_DATABASE'),
        password=os.environ.get('PG_DATA_WAREHOUSE_DATABASE_PASSWORD'),
        host=os.environ.get('PG_DATA_WAREHOUSE_HOST'),
        port=os.environ.get('PG_DATA_WAREHOUSE_PORT')
    )

    try:
        # Tạo cursor
        cur = conn.cursor()

        # Cập nhật START_TS và STATUS cho job_name cụ thể
        cur.execute("""
            UPDATE etl.etl_job
            SET start_ts = NOW(),
                status = -1
            WHERE job_name = %s
        """, (job_name,))

        # Cam kết thay đổi
        conn.commit()

        # Kiểm tra xem có dòng nào được cập nhật không
        if cur.rowcount == 0:
            print(f"Không tìm thấy job với tên: {job_name}")
        else:
            print(f"Cập nhật thành công cho job: {job_name}")

    except psycopg2.Error as e:
        print(f"Lỗi khi cập nhật job: {e}")
        conn.rollback()

    finally:
        # Đóng cursor và kết nối
        cur.close()
        conn.close()

def end_job(job_name):
    # Kết nối đến database (thay bằng thông tin thực tế của bạn)
    conn = psycopg2.connect(
        dbname=os.environ.get('PG_DATA_WAREHOUSE_DATABASE'),
        user=os.environ.get('PG_DATA_WAREHOUSE_DATABASE'),
        password=os.environ.get('PG_DATA_WAREHOUSE_DATABASE_PASSWORD'),
        host=os.environ.get('PG_DATA_WAREHOUSE_HOST'),
        port=os.environ.get('PG_DATA_WAREHOUSE_PORT')
    )

    try:
        # Tạo cursor
        cur = conn.cursor()

        # Cập nhật END_TS và STATUS cho job_name cụ thể
        cur.execute("""
            UPDATE etl.etl_job
            SET end_ts = NOW(),
                status = 1
            WHERE job_name = %s
        """, (job_name,))

        # Cam kết thay đổi
        conn.commit()

        # Kiểm tra xem có dòng nào được cập nhật không
        if cur.rowcount == 0:
            print(f"Không tìm thấy job với tên: {job_name}")
        else:
            print(f"Cập nhật thành công cho job: {job_name}")

    except psycopg2.Error as e:
        print(f"Lỗi khi cập nhật job: {e}")
        conn.rollback()

    finally:
        # Đóng cursor và kết nối
        cur.close()
        conn.close()