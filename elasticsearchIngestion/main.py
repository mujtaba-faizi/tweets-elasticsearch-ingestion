import os
import json
from dotenv import load_dotenv
import tarfile
import time
import shutil
from util import deleteAllDocs, sendPostRequest

start = time.time()

load_dotenv()
DATA_PATH = os.getenv("DATA_PATH")
URL = os.getenv("URL")
EXTRACTED_DIR = os.getenv("EXTRACTED_DIR")
BUFFER_SIZE = 1000
ZIPS = ["0300.tar.gz","0301.tar.gz","0302.tar.gz","0303.tar.gz","0304.tar.gz","0305.tar.gz","0306.tar.gz","0307.tar.gz","0308.tar.gz","0309.tar.gz","0310.tar.gz","0311.tar.gz","0312.tar.gz","0313.tar.gz","0314.tar.gz","0315.tar.gz","0316.tar.gz","0317.tar.gz","0318.tar.gz","0319.tar.gz","0320.tar.gz","0321.tar.gz","0322.tar.gz","0323.tar.gz","0324.tar.gz","0325.tar.gz","0326.tar.gz","0327.tar.gz","0328.tar.gz","0329.tar.gz","0330.tar.gz","0331.tar.gz","0332.tar.gz","0333.tar.gz","0334.tar.gz","0335.tar.gz","0336.tar.gz","0337.tar.gz","0338.tar.gz","0339.tar.gz","0340.tar.gz","0341.tar.gz","0342.tar.gz","0343.tar.gz","0344.tar.gz","0345.tar.gz","0346.tar.gz","0347.tar.gz","0348.tar.gz","0349.tar.gz","0350.tar.gz","0351.tar.gz","0352.tar.gz","0353.tar.gz","0354.tar.gz","0355.tar.gz","0356.tar.gz","0357.tar.gz","0358.tar.gz","0359.tar.gz","0360.tar.gz"]

# check if have to reindex data if memory runs out and elasticsearch crashes
# word clouds for trending hash tags


def extract():
    min_entries = os.listdir(DATA_PATH)  # list of all minute folder names
    for zip in min_entries:
        if zip in ZIPS:  # extract only these minute folders
            print(DATA_PATH + "/" + zip)
            file = tarfile.open(DATA_PATH + "/" + zip)
            file.extractall("./extraction_dir/" + zip)
            file.close()
            index(EXTRACTED_DIR)
            # delete the extracted minute folder
            shutil.rmtree(EXTRACTED_DIR + zip)


def index(path):
    min_entries = os.listdir(path)  # list of all minute folder names
    docs = []
    bulk_data = ""
    # incrementing for bulk requests matching the buffer size
    count = 0
    # for incrementing index based counts
    index_count = 0
    for min_file in min_entries:  # iterate over all minute files
        min_path = path + min_file + "/"
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
                        doc["timestamp_minute"] = int(min_file[:4])
                        docs.append(doc)
                        count += 1
                        # Bulk index the data
                        if count > BUFFER_SIZE:
                            for data in docs:
                                index_count += 1
                                bulk_data += json.dumps({"index": {"_id": index_count}}) + "\n"
                                # add the document data
                                bulk_data += json.dumps(data) + "\n"
                            sendPostRequest(URL, bulk_data)
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
    sendPostRequest(URL, bulk_data)
    # Reset the bulk_data variable


# deleteDoc("0K1zmYcBf-vfNQzqUNj1")
deleteAllDocs()
# index()
extract()

end = time.time()
print((end - start)/60, " min")
