import os
import gzip
import json
import requests
from requests.auth import HTTPBasicAuth


def sendPostRequest(request, data):
    if AUTH is None:
        r = requests.post(request, headers=HEADERS, data=data, )
    else:
        r = requests.post(request, headers=HEADERS, data=data, auth=AUTH, verify=VERIFY)
    print(r.text)


def deleteDoc(doc_id):
    if AUTH is None:
        r = requests.delete(URL + "/" + doc_id, headers=HEADERS,)
    else:
        r = requests.delete(URL + "/" + doc_id, headers=HEADERS, auth=AUTH, verify=VERIFY)
    print(r.text)


def deleteAllDocs():
    data = "{ \"query\": { \"match_all\": {} } }"
    sendPostRequest(URL[:-5] + "/_delete_by_query?conflicts=proceed", data)


def index():
    min_entries = os.listdir(ABS_PATH)  # list of all minute folder names
    for min_file in min_entries:  # iterate over all minute files
        min_path = ABS_PATH+min_file+"/"
        sec_entries = os.listdir(min_path)  # list of all second folder names
        for sec_file in sec_entries:  # iterate over all second files
            with open(min_path + sec_file, 'r', encoding='utf8') as file:
                for line in file:
                    json_data = json.loads(line)
                    data = json_data["data"]
                    data = {'data': data, "second_timestamp": int(sec_file[-7:-5]), 'minute_timestamp': int(min_file)}
                    sendPostRequest(URL, data)
                    #break
            #break
        #break


# deleteDoc("0K1zmYcBf-vfNQzqUNj1")
deleteAllDocs()
index()