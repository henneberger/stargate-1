

import os
import json
import requests

url = "http://localhost:8080"
#url = "https://sandbox.non-prod.appstax.com"

def printResult(result):
    print(json.dumps(json.loads(result.text), indent=True))


def push(counter = 0):
    counter += 1
    current_dir = os.path.dirname(os.path.abspath(__file__))
    schema_conf = "main/resources/schema.conf"
    full_path = os.path.join(current_dir, "..", "..", schema_conf)
    print ("opening file at " + full_path)
    full_url = url + "/test"
    print("posting to " + full_url)
    resp = requests.post(full_url, data=str(open(full_path).read()),  headers={"content-type":"multipart/form-data"})
    print(resp.headers)
    if resp.status_code != 200:
        if counter > 10:
            print('failed too many times baiiling out: Response code: ' + str(resp))
            return
        print('failed posting schema, trying again. Response code: ' + str(resp))
        push(counter)


def create():
    result = requests.post(url + "/test/Customer", json={
        "firstName": "Steve",
        # relation ops are: [link, unlink, replace], if none of these are specified, defaults to replace
        "addresses": {
            # child entities (to be linked, unlinked, etc) can be either creates (new) or updates (existing)
            # if neither create nor update are specified, defaults to crete
            "street": "kent st",
            "zipCode":"22046"
        },
        "orders":[
            # links parent customer to a new order, which is linked to any existing products with the name "widget"
            {
                "time": 12345,
                "products": {
                        "-update": {
                            "-match": ["name", "=", "widget"]
                        }
                }
            },
            # links parent customer to a new order with no products
            # this is the long-form version of "products": [], (since default behavior is /replace/create)
            {
                "total": 0,
                "products": {
                        "-create": []
                }
            }
        ]
    }, headers={"content-type":"application/json"})
    printResult(result)

def get():
    # get all Customers with firstName=Daniel, include any related addresses and orders in results
    result = requests.get(url + "/test/Customer", json={
        "-match":["firstName","=", "Steve"],
        "addresses":{},
        "orders":{}}, headers={"content-type":"application/json"})
    printResult(result)

def update():
    result = requests.put(url + "/test/Customer", json={
        "-match":["firstName","=","Steve"],
        "lastName": "Danger",
        "addresses":{
            "-update":{
                "-match":["customers.firstName","=","Steve"],
                "street":"other st"}}}, headers={"content-type":"application/json"})
    printResult(result)

def predefinedGet(name = "Steve"):
    result = requests.get(url + "/test/q/customerByFirstName", json={
        "-match":{
            "customerName": name
        }
    }, headers={"content-type":"application/json"})
    printResult(result)



def demo1():
    push()
    print("\n\ntest 1")
    create()
    get()
    print("\n\ntest 2")
    update()
    get()
    print("\n\ntest 3")
    create()
    get()
    print("\n\ntest 4")
    predefinedGet()

if __name__ == "__main__":
    demo1()
