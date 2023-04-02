import datetime

import requests
import xml.etree.ElementTree as ET


def convert_valute_to_RUB(url: str, valute_code: str) -> float:
    response = requests.get(url)
    tree = ET.fromstring(response.text)

    for valute in tree:
        if valute.attrib["ID"] == valute_code:
            nominal = float(valute.find("Nominal").text)
            value = float(valute.find("Value").text.replace(",", "."))
            return value / nominal


def convert_str_to_date_object(date: str) -> datetime:
    day, month, year = date.split(".")
    correct_format = f"{year}-{month}-{day}"

    return datetime.date.fromisoformat(correct_format)


if __name__ == "__main__":
    convert_valute_to_RUB()
