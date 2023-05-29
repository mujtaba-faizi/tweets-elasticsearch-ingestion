import os
import gzip
import json
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import tarfile

load_dotenv()
URL = os.getenv("URL")
ABS_PATH = os.getenv("ABS_PATH")
HEADERS = {'Content-Type': 'application/json', 'Authorization': os.getenv("API_KEY"), }
AUTH = os.getenv("AUTH")
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
    sendPostRequest(URL[:-5] + "/_delete_by_query?conflicts=proceed", data)


def extract():
    min_entries = os.listdir(ABS_PATH)  # list of all minute folder names
    for zip in min_entries:
        if zip == "0300":  # skip this folder
            continue
        print(ABS_PATH + "/" + zip)
        file = tarfile.open(ABS_PATH + "/" + zip)
        file.extractall("./extraction_dir/" + zip)
        file.close()


def index():
    # count=0
    min_entries = os.listdir(ABS_PATH)  # list of all minute folder names
    for min_file in min_entries:  # iterate over all minute files
        min_path = ABS_PATH + min_file + "/"
        sec_entries = os.listdir(min_path)  # list of all second folder names
        dir(min_path)  # list of all second folder names
        for sec_file in sec_entries:  # iterate over all second files
            with open(min_path + sec_file, 'r', encoding='utf8') as file:
                for line in file:
                    json_data = json.loads(line)
                    data = json_data["data"]
                    try:
                        places = json_data["includes"]["places"]
                    except:
                        pass
                    for doc in data:  # array of json documents
                        try:
                            place_id = doc["geo"]["place_id"]
                            for place in places:
                                if place["id"] == place_id:
                                    # count = count + 1
                                    # print(count)
                                    doc["place"] = place
                                    bbox = doc["place"]["geo"]["bbox"]
                                    # if count<7:
                                    #     doc["place"]["geo"]["bbox"] = {"type": "envelope",
                                    #                                    "coordinates": [[-67.1425907, 10.6059297],[-66.9123794, 10.5178096]]}
                                    #
                                    # else:
                                    doc["place"]["geo"]["bbox"] = {"type": "envelope", "coordinates": [[float(bbox[0]), float(bbox[3])], [float(bbox[2]), float(bbox[1])]]}  #envelope
                                        # print(bbox[2], bbox[3], bbox[0], bbox[1])
                                        # print(doc["place"]["country"])
                                        # [[bbox[2], bbox[3]], [bbox[0], bbox[1]]]}
                        except:
                            pass
                        # doc["place"] = {"geo": {"bbox": {"type" : "envelope", "coordinates":[[-77.03653, 38.897676], [-77.009051, 38.889939]]}, "type":"feature"}, "country":"Spanien"}
                        doc["timestamp_second"] = int(sec_file[-7:-5])
                        doc["timestamp_minute"] = int(min_file)
                        r = requests.post(URL, headers=HEADERS, json=doc, )
                        print(r.text)
                        # break
                    # data = {'data': data, "second_timestamp": int(sec_file[-7:-5]), 'minute_timestamp': int(min_file)}
                    # r = requests.post(URL, headers=HEADERS, json=data,)
                    # print(r.text)
                    # break
        #     break
        # break


# deleteDoc("0K1zmYcBf-vfNQzqUNj1")
deleteAllDocs()
index()
# extract()
