import time
import requests
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost:27017/")
db = client["car_database"]
collection = db["cars"]  

def is_id_processed(car_id):
    return collection.find_one({"id": car_id}) is not None

def get_car_list(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        car_listings = soup.find_all('div', class_='products-i')
        for listing in car_listings:
            car_id = re.search(r'/(?P<number>\d+)-', listing.find(class_="products-i__link")['href']).group('number') if re.search(r'/(?P<number>\d+)-', listing.find(class_="products-i__link")['href']) else None
            
            if car_id and not is_id_processed(car_id):
                car_info = {
                    '_id': car_id,
                    'id': car_id,
                    'products-i__link': listing.find(class_="products-i__link")['href'],
                    'attributes_products-i__bottom-text': listing.find(class_='products-i__attributes products-i__bottom-text').text.strip(),
                    'products-i__top': listing.find(class_="products-i__top").find('img')['src'],
                    'product-price': listing.find(class_="product-price").text.strip(),
                    'products-i__datetime': listing.find(class_="products-i__datetime").text.strip(),
                    'products-i__tooltip products-i__tooltip--loan': listing.find(class_="products-i__tooltip products-i__tooltip--loan").text if listing.find(class_="products-i__tooltip products-i__tooltip--loan") else None,
                    'products-i__label products-i__label--salon': listing.find(class_="products-i__label products-i__label--salon").text if listing.find(class_="products-i__label products-i__label--salon") else None,
                    'products-i__tooltip--barter': listing.find(class_="products-i__tooltip products-i__tooltip--barter").text if listing.find(class_="products-i__tooltip products-i__tooltip--barter") else None,
                    'name_products-i__bottom-text': listing.find(class_="products-i__name products-i__bottom-text").text,
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
                collection.insert_one(car_info)
                print(car_info)
    else:
        print("Error: Unable to fetch data from the website")

if __name__ == "__main__":
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    url = 'https://turbo.az/autos'
    
    while True:
        get_car_list(url, headers)

        time.sleep(30)
