import os
from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth


load_dotenv()
URL = os.getenv("URL")
HEADERS = {'Content-Type': 'application/json', }#'Authorization': os.getenv("API_KEY"), }
AUTH = HTTPBasicAuth(os.getenv("USER"), os.getenv('PASSWORD'))
VERIFY = os.getenv("VERIFY")


def sendPostRequest(request, data):
    if AUTH == "":
        r = requests.post(request, headers=HEADERS, data=data, )
    else:
        r = requests.post(request, headers=HEADERS, data=data, auth=AUTH, verify=VERIFY)
    print(r.text)


def deleteDoc(doc_id):
    if AUTH == "":
        r = requests.delete(URL + "/" + doc_id, headers=HEADERS, )
    else:
        r = requests.delete(URL + "/" + doc_id, headers=HEADERS, auth=AUTH, verify=VERIFY)
    print(r.text)


def deleteAllDocs():
    data = "{ \"query\": { \"match_all\": {} } }"
    sendPostRequest(URL[:-6] + "/_delete_by_query?conflicts=proceed", data)