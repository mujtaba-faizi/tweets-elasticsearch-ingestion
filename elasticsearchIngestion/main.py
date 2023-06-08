import os
import gzip
import json
from dotenv import load_dotenv
import tarfile
import time

from util import deleteAllDocs, sendPostRequest

start = time.time()

load_dotenv()
ABS_PATH = os.getenv("ABS_PATH")
URL = os.getenv("URL")
BUFFER_SIZE = 1000


# check if have to reindex data if memory runs out and elasticsearch crashes
# word clouds for trending hash tags


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
    min_entries = os.listdir(ABS_PATH)  # list of all minute folder names
    docs = []
    bulk_data = ""
    # incrementing for bulk requests matching the buffer size
    count = 0
    # for incrementing index based counts
    index_count = 0
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
                                    doc["place"] = place
                                    bbox = doc["place"]["geo"]["bbox"]
                                    doc["place"]["geo"] = doc["place"]["geo"] = \
                                        {"bbox": {"type": "envelope", "coordinates": [[float(bbox[0]), float(bbox[3])],
                                                                                      [float(bbox[2]),
                                                                                       float(bbox[1])]]},
                                         "center": {"type": "point",
                                                    "coordinates": [(float(bbox[0]) + float(bbox[2])) / 2,
                                                                    (float(bbox[3]) + float(bbox[1])) / 2]}}
                        except:
                            pass
                        doc["timestamp_second"] = int(sec_file[-7:-5])
                        doc["timestamp_minute"] = int(min_file)
                        docs.append(doc)
                        count += 1
                        # Bulk index the data
                        if count > BUFFER_SIZE:
                            for data in docs:
                                index_count += 1
                                bulk_data += json.dumps({"index": {"_id": index_count}}) + "\n"
                                # add the document data
                                bulk_data += json.dumps(data) + "\n"
                            r = sendPostRequest(URL, bulk_data)
                            # reset the bulk_data and docs variables
                            bulk_data = ""
                            docs = []
                            count = 0
                        # break
                    # break
        #     break
        # break
    for data in docs:
        index_count += 1
        bulk_data += json.dumps({"index": {"_id": index_count + 1}}) + "\n"
        # Add the document data
        bulk_data += json.dumps(data) + "\n"
    r = sendPostRequest(URL, bulk_data)
    # Reset the bulk_data variable


# deleteDoc("0K1zmYcBf-vfNQzqUNj1")
deleteAllDocs()
index()
# extract()

end = time.time()
print(end - start)
