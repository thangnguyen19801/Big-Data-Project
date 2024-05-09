import requests
import json
import csv
import re

phone_and_laptop_page_url = "https://tiki.vn/api/personalish/v1/blocks/listings?limit=2&page=2&category=1789&urlKey=dien-thoai-may-tinh-bang"
product_url = "https://tiki.vn/api/v2/products/{}"

product_id_file = "./data/product-id.txt"
product_data_file = "./data/product.txt"
product_file = "./data/product.csv"

headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}


def crawl_product_id(url):
    product_list = []
    print(url)
    response = requests.get(url, headers=headers)
    
    if (response.status_code != 200):
        return []

    products = json.loads(response.text)["data"]

    if (len(products) == 0):
        return []

    for product in products:
        product_id = str(product["id"])
        print("Product ID: ", product_id)
        product_list.append(product_id)


    return product_list

def crawl_product(product_list=[]):
    product_detail_list = []
    for product_id in product_list:
        response = requests.get(product_url.format(product_id), headers=headers)
        if (response.status_code == 200):
            product = json.loads(response.text)
            product_detail_list.append(product)
            print("Crawl product: ", product_id, ": ", response.status_code)
    return product_detail_list

flatten_field = [ "badges", "inventory", "categories", "rating_summary", 
                      "brand", "seller_specifications", "current_seller", "other_sellers", 
                      "configurable_options",  "configurable_products", "specifications", "product_links",
                      "services_and_promotions", "promotions", "stock_item", "installment_info" ]

def adjust_product(product):
    e = dict(product)
    for field in flatten_field:
        if field in e:
            e[field] = json.dumps(e[field], ensure_ascii=False).replace('\n','')

    return e








    
