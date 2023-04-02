import time
from typing import List
import apiclient
import httplib2
from oauth2client.service_account import ServiceAccountCredentials

from config import CREDENTIALS_FILE, SPREADSHEET_ID
from config import CBR_URL, CBR_USD_CODE
from converter import convert_valute_to_RUB, convert_str_to_date_object
from db.psql_tools import add_orders_to_db, update_orders_in_db, get_all_orders_nums_from_db, get_order_from_db, delete_all_orders_from_db, delete_orders_from_db


def connect_to_google_sheets():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ["https://www.googleapis.com/auth/spreadsheets"]
    )
    httpAuth = credentials.authorize(httplib2.Http())
    return apiclient.discovery.build("sheets", "v4", http=httpAuth)


def get_google_sheets():
    service = connect_to_google_sheets()

    # read file
    values = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="Лист1!A2:D1000",
        majorDimension="ROWS"
    ).execute()

    if values.get("values", None) is None:
        delete_all_orders_from_db()
        return None

    usd_rate_to_rub = convert_valute_to_RUB(CBR_URL, CBR_USD_CODE)
    db_orders_nums: List[int] = [order[0] for order in get_all_orders_nums_from_db()]

    new_orders: List[tuple] = []
    update_orders: List[tuple] = []
    orders_from_google_sheets: List[int] = []
    delete_orders: List[int] = []
    for value in values["values"]:
        number, order_num, price_usd, delivery_time = value

        number, order_num, price_usd = int(number), int(order_num), float(price_usd)
        price_rub = price_usd * usd_rate_to_rub

        if order_num not in db_orders_nums:
            delivery_time = convert_str_to_date_object(delivery_time)
            new_orders.append((number, order_num, price_usd, price_rub, delivery_time))

        elif (number, order_num, price_usd, delivery_time) != get_order_from_db(order_num):
            delivery_time = convert_str_to_date_object(delivery_time)
            update_orders.append((number, order_num, price_usd, price_rub, delivery_time))

        orders_from_google_sheets.append(order_num)

    delete_orders.extend(
        db_order_num
        for db_order_num in db_orders_nums
        if db_order_num not in orders_from_google_sheets
    )

    add_orders_to_db(new_orders)
    update_orders_in_db(update_orders)
    delete_orders_from_db(delete_orders)


if __name__ == "__main__":
    while True:
        get_google_sheets()
        time.sleep(8)
