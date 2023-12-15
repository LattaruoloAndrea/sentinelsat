from sentinelsat.sentinel import SentinelAPI,geojson_to_wkt,read_geojson,read_collections,remove_geometry_collection
import threading
from concurrent.futures import ThreadPoolExecutor

api = SentinelAPI("alattaruolo@fbk.eu", "2CKb!#urVFbGUa4")
print(api.session.auth[0])
# print(api.token)
start_date = "2023-09-12T00:00:00.000Z" # 2023-09-12T00:00:00.000Z, 2022-06-01T00:00:00.000Z 
end_date = "2023-09-18T14:14:00.450Z" # 2023-09-18T14:14:00.450Z, 2022-06-10T23:59:59.999Z
data_collection = "SENTINEL-1"
aoi = "POLYGON((4.220581 50.958859,4.521264 50.953236,4.545977 50.906064,4.541858 50.802029,4.489685 50.763825,4.23843 50.767734,4.192435 50.806369,4.189689 50.907363,4.220581 50.958859))"
# aoi = "POLYGON((9.4459 42.2358,9.4459 42.2602,9.4816 42.2602,9.4816 42.2358,9.4459 42.2358))"
tileid = ""
wkt = geojson_to_wkt(read_geojson("/home/dsl/Documents/fbk/dslab.sentinel/sentinel-back-end/search_polygon.geojson"))
wkt = read_collections(remove_geometry_collection(wkt))[0]
print(wkt)
# 2023-09-12T00:00:00Z 2023-09-18T14:14:45Z <class 'str'>
# Item 015 030320 IW1: search for products acquired from 2023-09-12T00:00:00Z and 2023-09-18T14:14:45Z inclusive
# footprint GEOMETRYCOLLECTION(POLYGON((9.4459 42.2358,9.4459 42.2602,9.4816 42.2602,9.4816 42.2358,9.4459 42.2358)))
query = api.query_sentinel_1(start_date=start_date,end_date=end_date,point=wkt)
print(query)
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
r = api.download_all(query,directory_path="/media/dsl/1A2226C62D41B5A2/donwload_data/try_script/miao/")
print("FINITO @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@", r)
# print(c.downloader.api.downloader.api.downloader)

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
# https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq 'SENTINEL-1' and ContentDate/Start gt 2023-09-12T00:00:000Z and ContentDate/Start lt 2023-09-18T14:14:450Z and OData.CSC.Intersects(area=geography'SRID=4326;POLYGON((9.4459 42.2358,9.4459 42.2602,9.4816 42.2602,9.4816 42.2358,9.4459 42.2358))) and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq 'SLC') and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'operationalMode' and att/OData.CSC.StringAttribute/Value eq 'IW')&$orderby=ContentDate/Start asc