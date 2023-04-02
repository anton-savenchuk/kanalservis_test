from contextlib import contextmanager
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
from typing import List

from config import PG_HOST, PG_DB, PG_USER, PG_PASSWORD


def create_orders_table():
    psql = """
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            number integer NOT NULL,
            order_num integer NOT NULL,
            price_usd real NOT NULL,
            price_rub real NOT NULL,
            delivery_time DATE NOT NULL,
            delivery_completed boolean NOT NULL DEFAULT FALSE
        );"""

    with get_cursor() as cursor:
        cursor.execute(psql)


def get_db_connection() -> psycopg2.extensions.connection:
    try:
        connection = psycopg2.connect(
            host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASSWORD
        )
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    except (Exception, Error) as error:
        print("Postgrepsql error", error)
    finally:
        return connection


@contextmanager
def get_cursor():
    connection = get_db_connection()
    cursor = connection.cursor()
    yield cursor
    cursor.close()
    connection.close()


def add_orders_to_db(orders: List[tuple]) -> None:
    if not orders:
        return None
    psql = """
        INSERT INTO orders
        (number, order_num, price_usd, price_rub, delivery_time)
        VALUES %s;"""

    with get_cursor() as cursor:
        execute_values(cursor, psql, orders)


def update_orders_in_db(orders: List[tuple]) -> None:
    if not orders:
        return None
    with get_cursor() as cursor:
        psql = """
            UPDATE orders
            SET number = %(number)s, price_usd = %(price_usd)s, price_rub = %(price_rub)s, delivery_time = %(delivery_time)s
            WHERE order_num = %(order_num)s;"""

        for order in orders:
            number, order_number, price_usd, price_rub, delivery_time = order
            values = {
                "number": number,
                "order_num": order_number,
                "price_usd": price_usd,
                "price_rub": price_rub,
                "delivery_time": delivery_time,
            }

            cursor.execute(psql, values)


def get_all_orders_nums_from_db() -> List[tuple]:
    psql = """
        SELECT order_num
        FROM orders;"""

    with get_cursor() as cursor:
        cursor.execute(psql)
        return cursor.fetchall()


def get_order_from_db(order_num: int):
    with get_cursor() as cursor:
        psql = """
            SELECT number, order_num, price_usd, to_char(delivery_time, 'DD.MM.YYYY')
            FROM orders
            WHERE order_num = %(order_num)s;"""

        values = {"order_num": order_num}
        cursor.execute(psql, values)
        return cursor.fetchone()


def delete_all_orders_from_db() -> None:
    psql = "DELETE FROM orders"

    with get_cursor() as cursor:
        cursor.execute(psql)


def delete_orders_from_db(order_nums: List[int]):
    if not order_nums:
        return None

    with get_cursor() as cursor:
        psql = """
            DELETE FROM orders
            WHERE order_num = %(order_num)s;"""

        for order_num in order_nums:
            values = {"order_num": order_num}
            cursor.execute(psql, values)


if __name__ == "__main__":
    print("Creating orders table...")
    create_orders_table()
    print("Creating orders table is completed.")
