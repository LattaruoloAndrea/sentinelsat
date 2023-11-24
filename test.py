from sentinelsat.sentinel import SentinelAPI
import threading
from concurrent.futures import ThreadPoolExecutor

api = SentinelAPI("alattaruolo@fbk.eu", "2CKb!#urVFbGUa4")
print(api.session.auth[0])
# print(api.token)
start_date = "2022-06-01T00:00:00.000Z"
end_date = "2022-06-10T23:59:59.999Z"
data_collection = "SENTINEL-1"
aoi = "POLYGON((4.220581 50.958859,4.521264 50.953236,4.545977 50.906064,4.541858 50.802029,4.489685 50.763825,4.23843 50.767734,4.192435 50.806369,4.189689 50.907363,4.220581 50.958859))'"
order = "asc"
type_ = "SLC"
mode = "IW"
tileid = ""
query = api.query_sentinel_1(start_date=start_date,end_date=end_date,point=aoi)
# query = api.query_sentinel_2(start_date,end_date,tileid)
# import json
# od2 = json.loads(json.dumps(query))
# df = api.to_dataframe(od2)
# print(df['Id'])
# query = api.query(start_date=start_date,end_date=end_date,data_collection=data_collection,aoi=aoi,order_by=order) # ,order_by=order
# print(query)
# for i in query:
#     for key in i.keys():
#         print(key)
# print(query['Checksum'])
# /home/dsl/Downloads
# /media/dsl/1A2226C62D41B5A2/donwload_data/try_script/
api.download_all(query,directory_path="/media/dsl/1A2226C62D41B5A2/donwload_data/try_script/")
#print(c.downloader.api.downloader.api.downloader)

# semaphore = threading.BoundedSemaphore(4)
# dl_executor = ThreadPoolExecutor(
#             max_workers=max(1, 2),
#             thread_name_prefix="dl",
#         )

# url_ = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=not contains(Name,'S2') and ContentDate/Start gt 2022-05-03T00:00:00.000Z and ContentDate/Start lt 2022-05-03T00:10:00.000Z&$orderby=ContentDate/Start&$top=100"
# import requests
# session_ = requests.Session()
# headers = {}
# r_ = requests.get('https://httpbin.org/get',headers=headers)
# print(r_)
# 
# def ff(inte):
#     import random
#     import time
#     seconds = random.randint(1,5)
#     time.sleep(seconds)
#     session = requests.Session()
#     with semaphore:
#         url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=not contains(Name,'S2') and ContentDate/Start gt 2022-05-03T00:00:00.000Z and ContentDate/Start lt 2022-05-03T00:10:00.000Z&$orderby=ContentDate/Start&$top=100"
#         # session.headers.update(headers)
#         # print(headers)
#         print('before')
#         r = session.get(url)
#         print(r)
#         print(inte)
# # 
# for i in range(5):
#     dl_executor.submit(ff,i)