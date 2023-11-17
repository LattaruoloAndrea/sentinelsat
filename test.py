from sentinelsat.sentinel import SentinelAPI


api = SentinelAPI("alattaruolo@fbk.eu", "2CKb!#urVFbGUa4")
start_date = "2022-06-01"
end_date = "2022-06-10"
data_collection = "SENTINEL-1"
aoi = "POLYGON((4.220581 50.958859,4.521264 50.953236,4.545977 50.906064,4.541858 50.802029,4.489685 50.763825,4.23843 50.767734,4.192435 50.806369,4.189689 50.907363,4.220581 50.958859))'"

query = api.query(start_date=start_date,end_date=end_date,data_collection=data_collection,aoi=aoi)
# print(query['Id'])
api.download_all(query,directory_path="/media/dsl/1A2226C62D41B5A2/donwload_data/try_script/")
#print(c.downloader.api.downloader.api.downloader)