import hashlib
import logging
import re
import threading
import pandas as pd
import xml.etree.ElementTree as ET
from collections import OrderedDict, defaultdict, namedtuple
from copy import copy
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict
from urllib.parse import quote_plus, urljoin
import time
import geojson
import geomet.wkt
import html2text
import requests
from tqdm.auto import tqdm

from sentinelsat.download import DownloadStatus, Downloader
from sentinelsat.exceptions import (
    InvalidAoi,
    InvalidChecksumError,
    InvalidDataCollection,
    InvalidDirection,
    InvalidFormatDate,
    InvalidKeyError,
    InvalidMode,
    InvalidOrbit,
    InvalidOrderBy,
    InvalidPLevel,
    InvalidProductType,
    InvalidTileId,
    QueryLengthError,
    QuerySyntaxError,
    SentinelAPIError,
    ServerError,
    UnauthorizedError,
)
from sentinelsat.helper import correct_data_collections, correct_format_date, valid_aoi, valid_direction, valid_mode, valid_orbit, valid_order_by, valid_plevel, valid_product_type, valid_tileId
from . import __version__ as sentinelsat_version


class SentinelAPI:
    """Class to connect to Copernicus Open Access Hub, search and download imagery.

    Parameters
    ----------
    user : string
        username for DataHub
        set to None to use ~/.netrc
    password : string
        password for DataHub
        set to None to use ~/.netrc
    api_url : string, optional
        URL of the DataHub
        defaults to 'https://apihub.copernicus.eu/apihub'
    show_progressbars : bool
        Whether progressbars should be shown or not, e.g. during download. Defaults to True.
    timeout : float or tuple, default 60
        How long to wait for DataHub response (in seconds).
        Tuple (connect, read) allowed.
        Set to None to wait indefinitely.

    Attributes
    ----------
    session : requests.Session
        Session to connect to DataHub
    api_url : str
        URL to the DataHub
    page_size : int
        Number of results per query page.
        Current value: 100 (maximum allowed on ApiHub)
    timeout : float or tuple
        How long to wait for DataHub response (in seconds).
    """

    logger = logging.getLogger("sentinelsat.SentinelAPI")

    def __init__(
        self,
        user,
        password,
        identity_api_url="https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        show_progressbars=True,
        timeout=60,
    ):
        self.session = requests.Session()
        if user and password:
            self.session.auth = (user, password)
        self.identity_api_url = identity_api_url if identity_api_url.endswith("/") else identity_api_url + "/"
        self.page_size = 100
        self.user_agent = "sentinelsat/" + sentinelsat_version
        self.session.headers["User-Agent"] = self.user_agent
        self.session.timeout = timeout
        self.show_progressbars = show_progressbars
        self._dhus_version = None
        self.time_stamp_login = 0
        self.token,self.refresh_token,self.token_expires_in,self.refresh_expires_in = self._get_access_token(user,password)
        # For unit tests
        self._last_query = None
        self._last_response = None
        self._online_attribute_used = True

        self._concurrent_dl_limit = 4
        self._concurrent_lta_trigger_limit = 10

        # The number of allowed concurrent GET requests is limited on the server side.
        # We use a bounded semaphore to ensure we stay within that limit.
        # Notably, LTA trigger requests also count against that limit.
        self._dl_limit_semaphore = threading.BoundedSemaphore(self._concurrent_dl_limit)

        self.downloader = Downloader(self)

    @property
    def concurrent_dl_limit(self):
        """int: Maximum number of concurrent downloads allowed by the server."""
        return self._concurrent_dl_limit

    @concurrent_dl_limit.setter
    def concurrent_dl_limit(self, value):
        self._concurrent_dl_limit = value
        # self._lta_limit_semaphore = threading.BoundedSemaphore(self._concurrent_dl_limit)

    @property
    def dl_limit_semaphore(self):
        return self._dl_limit_semaphore
    
    def if_token_expired_refresh(self):
        current_timestamp  = time.time()
        min_10_in_s = self.token_expires_in # seconds
        if self.time_stamp_login +min_10_in_s- (60*2) > current_timestamp:
            #8 minutes not passed
            print("token not expired")
            pass
        else:
            min_60_in_s = self.refresh_expires_in # seconds
            if self.time_stamp_login +min_60_in_s > current_timestamp:
                #refresh token still valid
                print("token expired use of the refresh token")
                self.token,self.refresh_token,self.token_expires_in,self.refresh_expires_in = self.refresh_access_token(self.refresh_token)
            else:
                #refresh token not valid
                print("token expired and refresh token expired")
                user = self.session.auth[0]
                password = self.session.auth[1]
                self.token,self.refresh_token,self.token_expires_in,self.refresh_expires_in = self._get_access_token(user,password)

    # @property
    # def dhus_version(self):
    #     if self._dhus_version is None:
    #         self._dhus_version = self._req_dhus_stub()
    #     return self._dhus_version
    
    def _get_access_token(self,username: str, password: str) -> str:
        data = {
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
        }
        try:
            r = requests.post(self.identity_api_url,
                            data=data,
                            )
            r.raise_for_status()
            self.time_stamp_login = time.time()
        except Exception as e:
            raise Exception(
                f"Access token creation failed. Reponse from the server was: {r.json()}"
            )
        json_ = r.json()
        return json_["access_token"],json_["refresh_token"],json_["expires_in"],json_["refresh_expires_in"]

    def refresh_access_token(self,refresh_token):
        data= {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': 'cdse-public'
        }
        header = {'Content-Type': 'application/x-www-form-urlencoded'}
        try:
            r = requests.post(self.identity_api_url,
                              headers=header,
                              data=data,
                            )
            r.raise_for_status()
            self.time_stamp_login = time.time()
        except Exception as e:
            print(f"Refresh token use failed. Reponse from the server was: {r.json()}")
            raise Exception(
                f"Refresh token use failed. Reponse from the server was: {r.json()}"
            )
        json_ = r.json()
        return json_["access_token"],json_["refresh_token"],json_["expires_in"],json_["refresh_expires_in"]

    def query_sentinel_1(self,start_date,end_date,point,direction=None,orbit=None):
        return self.query(start_date=start_date,end_date=end_date,order_by='asc',data_collection='SENTINEL-1',product_type='SLC',mode='IW',aoi=point,direction=direction,orbit=orbit)

    def query_sentinel_2(self,start_date,end_date,tile_id):
        return self.query(start_date=start_date,end_date=end_date,tileid=tile_id,order_by='asc',data_collection='SENTINEL-2',plevel='S2MSI2A')

    def query(
        self,
        start_date= None,
        end_date= None,
        data_collection= None,
        aoi= None,
        order_by= None,
        product_type = None,
        mode = None,
        direction = None,
        orbit = None,
        plevel = None,
        tileid = None,
    ):
        """Query the OpenSearch API with the coordinates of an area, a date interval
        and any other search keywords accepted by the API.

        Parameters
        ----------
        area : str, optional
            The area of interest formatted as a Well-Known Text string.
        date : tuple of (str or datetime) or str, optional
            A time interval filter based on the Sensing Start Time of the products.
            Expects a tuple of (start, end), e.g. ("NOW-1DAY", "NOW").
            The timestamps can be either a Python datetime or a string in one of the
            following formats:

                - yyyyMMdd
                - yyyy-MM-ddThh:mm:ss.SSSZ (ISO-8601)
                - yyyy-MM-ddThh:mm:ssZ
                - NOW
                - NOW-<n>DAY(S) (or HOUR(S), MONTH(S), etc.)
                - NOW+<n>DAY(S)
                - yyyy-MM-ddThh:mm:ssZ-<n>DAY(S)
                - NOW/DAY (or HOUR, MONTH etc.) - rounds the value to the given unit

            Alternatively, an already fully formatted string such as "[NOW-1DAY TO NOW]" can be
            used as well.
        raw : str, optional
            Additional query text that will be appended to the query.
        area_relation : {'Intersects', 'Contains', 'IsWithin'}, optional
            What relation to use for testing the AOI. Case insensitive.

                - Intersects: true if the AOI and the footprint intersect (default)
                - Contains: true if the AOI is inside the footprint
                - IsWithin: true if the footprint is inside the AOI

        order_by: str, optional
            A comma-separated list of fields to order by (on server side).
            Prefix the field name by '+' or '-' to sort in ascending or descending order,
            respectively. Ascending order is used if prefix is omitted.
            Example: "cloudcoverpercentage, -beginposition".
        limit: int, optional
            Maximum number of products returned. Defaults to no limit.
        offset: int, optional
            The number of results to skip. Defaults to 0.
        **keywords
            Additional keywords can be used to specify other query parameters,
            e.g. `relativeorbitnumber=70`.
            See https://scihub.copernicus.eu/twiki/do/view/SciHubUserGuide/3FullTextSearch
            for a full list.


        Range values can be passed as two-element tuples, e.g. `cloudcoverpercentage=(0, 30)`.
        `None` can be used in range values for one-sided ranges, e.g. `orbitnumber=(16302, None)`.
        Ranges with no bounds (`orbitnumber=(None, None)`) will not be included in the query.

        Multiple values for the same query parameter can be provided as sets and will be handled as
        logical OR, e.g. `orbitnumber={16302, 1206}`.

        The time interval formats accepted by the `date` parameter can also be used with
        any other parameters that expect time intervals (that is: 'beginposition', 'endposition',
        'date', 'creationdate', and 'ingestiondate').

        Returns
        -------
        dict[string, dict]
            Products returned by the query as a dictionary with the product ID as the key and
            the product's attributes (a dictionary) as the value.
        """
        query = self.format_query(start_date= start_date,end_date= end_date,data_collection= data_collection,aoi= aoi,order_by= order_by,product_type=product_type,mode=mode,direction=direction,orbit=orbit,tileid=tileid,plevel=plevel)
        # query = self.format_query(area, date, raw, area_relation, **keywords)

        if query.strip() == "":
            # An empty query should return the full set of products on the server, which is a bit unreasonable.
            # The server actually raises an error instead and it's better to fail early in the client.
            raise ValueError("Empty query.")

        self.logger.debug(
            "Running query: order_by=%s, start_date=%s, end_date=%s, query=%s",
            order_by,
            start_date,
            end_date,
            query,
        )
        odata_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/"
        return self.query_call_ordered_dict(odata_url,query=query)
        # formatted_order_by = _format_order_by(order_by)
        # response, count = self._load_query(query, formatted_order_by, limit, offset)
        # self.logger.info(f"Found {count:,} products")
        # return _parse_opensearch_response(response)

    @staticmethod
    def format_query(
        start_date= None,
        end_date= None,
        data_collection= None,
        aoi= None,
        product_type = None,
        mode = None,
        direction = None,
        orbit = None,
        plevel = None,
        tileid = None,
        order_by= None,
        ):
        """Create a OpenSearch API query string."""
        # k = f"Name eq '{data_collection}' and OData.CSC.Intersects(area=geography'SRID=4326;{aoi}) and ContentDate/Start gt {start_date}T00:00:00.000Z and ContentDate/Start lt {end_date}T00:00:00.000Z"
        pieces = []
        if data_collection != None:
            if correct_data_collections(data_collection):
                data_collection_query = f"$filter=Collection/Name eq '{data_collection}'"
                pieces.append(data_collection_query)
            else:
                error_msg = f"The data collection inderted is not one of the valid! {['Sentinel-1','Sentinel-2']}"
                raise(InvalidDataCollection(error_msg))
        if start_date != None:
            if correct_format_date(start_date):
                start_date_query = f"ContentDate/Start gt {start_date}"
                pieces.append(start_date_query)
            else:
                error_msg = f"The starting date inserted is not valid! {start_date} is not in the format required yyyy-mm-dd!"
                raise(InvalidFormatDate(error_msg))
        if end_date != None:
            if correct_format_date(end_date):
                end_date_query = f"ContentDate/Start lt {end_date}"
                pieces.append(end_date_query)
            else:
                error_msg = f"The ending date inserted is not valid! {end_date} is not in the format required yyyy-mm-dd!"
                raise(InvalidFormatDate(error_msg))
        if aoi!= None:
            if valid_aoi(aoi):
                aoi_query = f"OData.CSC.Intersects(area=geography'SRID=4326;{aoi}')"
                pieces.append(aoi_query)
            else:
                error_msg = "The aoi geometry inserted is not valid!"
                raise(InvalidAoi(error_msg))
        if product_type != None:
            if valid_product_type(product_type):
                product_type_query = f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '{product_type}')"
                pieces.append(product_type_query)
            else:
                error_msg = "The product type inserted is not valid!"
                raise(InvalidProductType(error_msg))
        if mode != None:
            if valid_mode(mode):
                mode_query = f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'operationalMode' and att/OData.CSC.StringAttribute/Value eq '{mode}')"
                pieces.append(mode_query)
            else:
                error_msg = "The mode inserted is not valid!"
                raise(InvalidMode(error_msg))
        if direction != None:
            if valid_direction(direction):
                direction_query = f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'orbitDirection' and att/OData.CSC.StringAttribute/Value eq '{direction}')"
                pieces.append(direction_query)
            else:
                error_msg = "The direction inserted is not valid!"
                raise(InvalidDirection(error_msg))
        if orbit != None:
            if valid_orbit(orbit):
                orbit_query = f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'relativeOrbitNumber' and att/OData.CSC.IntegerAttribute/Value eq {orbit})"
                pieces.append(orbit_query)
            else:
                error_msg = "The orbit inserted is not valid!"
                raise(InvalidOrbit(error_msg))
        if plevel != None:
            if valid_plevel(plevel):
                plevel_query = f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'processingLevel' and att/OData.CSC.StringAttribute/Value eq '{plevel}')"
                pieces.append(plevel_query)
            else:
                error_msg = "The plevel inserted is not valid!"
                raise(InvalidPLevel(error_msg))
        if tileid != None:
            if valid_tileId(tileid):
                tileId_query = f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'tileId' and att/OData.CSC.StringAttribute/Value eq '{tileid}'"
                pieces.append(tileId_query)
            else:
                error_msg = "The tileId by inserted is not valid!"
                raise(InvalidTileId(error_msg))    
        if order_by != None:
            if valid_order_by(order_by):
                order_by_query = f"&$orderby=ContentDate/Start {order_by}"
                pieces.append(order_by_query)
            else:
                error_msg = "The order_by by inserted is not valid!"
                raise(InvalidOrderBy(error_msg))
        else:
            # always order by ascending order if nothing provided
            order_by_query = f"&$orderby=ContentDate/Start asc"
            pieces.append(order_by_query)
        full_query = "Products?"
        for i in range(len(pieces)):
            full_query+= pieces[i]
            if i == len(pieces)-2 and valid_order_by(order_by):
                #order by query doen't need the "and"  
                pass
            elif i!= len(pieces)-1:
                full_query+= " and "
        return full_query
    
    def query_call(self,url=None,query=None):
        total_url = url
        print(f"{url}{query}")
        if query!= None:
            total_url += query
        json_ = requests.get(f"{url}{query}").json()
        if 'value' in json_:
            df = self.to_dataframe(json_["value"])
            return df
        else:
            df = self.to_dataframe(json_)
            return df
    
    def query_call_ordered_dict(self,url=None,query=None)-> OrderedDict:
        total_url = url
        print(f"{url}{query}")
        if query!= None:
            total_url += query
        json_ = requests.get(f"{url}{query}").json()
        if 'value' in json_:
            import json
            string_value = json.dumps(json_['value'])
            return json.JSONDecoder(object_pairs_hook=OrderedDict).decode(string_value)
        else:
            import json
            string_value = json.dumps(json_)
            return json.JSONDecoder(object_pairs_hook=OrderedDict).decode(string_value)

    
    def json_query_call(self,url):
        json_ = requests.get(f"{url}").json()
        return json_

    @staticmethod
    def to_geojson(products):
        """Return the products from a query response as a GeoJSON with the values in their
        appropriate Python types.
        """
        feature_list = []
        for i, (product_id, props) in enumerate(products.items()):
            props = props.copy()
            props["id"] = product_id
            poly = geomet.wkt.loads(props["footprint"])
            del props["footprint"]
            del props["gmlfootprint"]
            # Fix "'datetime' is not JSON serializable"
            for k, v in props.items():
                if isinstance(v, (date, datetime)):
                    props[k] = v.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            feature_list.append(geojson.Feature(geometry=poly, id=i, properties=props))
        return geojson.FeatureCollection(feature_list)

    @staticmethod
    def to_dataframe(products):
        """Return the products from a query response as a Pandas DataFrame
        with the values in their appropriate Python types.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("to_dataframe requires the optional dependency Pandas.")

        return pd.DataFrame.from_dict(products)
    
    @staticmethod
    def from_oreded_dict_to_dataframe(products):
        # try:
        import json
        od2 = json.loads(json.dumps(products))
        # except Exception as e:
        #     raise Exception(e)
        return SentinelAPI.to_dataframe(od2)

    @staticmethod
    def to_geodataframe(products):
        """Return the products from a query response as a GeoPandas GeoDataFrame
        with the values in their appropriate Python types.
        """
        try:
            import geopandas as gpd
            import shapely.wkt
        except ImportError:
            raise ImportError(
                "to_geodataframe requires the optional dependencies GeoPandas and Shapely."
            )

        crs = "EPSG:4326"  # WGS84
        if len(products) == 0:
            return gpd.GeoDataFrame(crs=crs, geometry=[])

        df = SentinelAPI.to_dataframe(products)
        geometry = [shapely.wkt.loads(fp) for fp in df["footprint"]]
        # remove useless columns
        df.drop(["footprint", "gmlfootprint"], axis=1, inplace=True)
        return gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

    def get_product_odata(self, id, full=False):
        # TODO get product Odata 
        """Access OData API to get info about a product.

        Returns a dict containing the id, title, size, md5sum, date, footprint and download url
        of the product. The date field corresponds to the Start ContentDate value.

        If `full` is set to True, then the full, detailed metadata of the product is returned
        in addition to the above.

        Parameters
        ----------
        id : string
            The UUID of the product to query
        full : bool
            Whether to get the full metadata for the Product. False by default.

        Returns
        -------
        dict[str, Any]
            A dictionary with an item for each metadata attribute

        Notes
        -----
        For a full list of mappings between the OpenSearch (Solr) and OData attribute names
        see the following definition files:
        https://github.com/SentinelDataHub/DataHubSystem/blob/master/addon/sentinel-1/src/main/resources/META-INF/sentinel-1.owl
        https://github.com/SentinelDataHub/DataHubSystem/blob/master/addon/sentinel-2/src/main/resources/META-INF/sentinel-2.owl
        https://github.com/SentinelDataHub/DataHubSystem/blob/master/addon/sentinel-3/src/main/resources/META-INF/sentinel-3.owl
        """
        url = self._get_odata_url(id, "?$format=json")
        products = self.json_query_call(url)
        return products

    def download(self, id, directory_path=".", checksum=True, nodefilter=None,proxy=None):
        """Download a product.

        Uses the filename on the server for the downloaded file, e.g.
        "S1A_EW_GRDH_1SDH_20141003T003840_20141003T003920_002658_002F54_4DD1.zip".

        Incomplete downloads are continued and complete files are skipped.

        Parameters
        ----------
        id : string
            UUID of the product, e.g. 'a8dd0cfd-613e-45ce-868c-d79177b916ed'
        directory_path : string, optional
            Where the file will be downloaded
        checksum : bool, default True
            If True, verify the downloaded file's integrity by checking its checksum.
            Throws InvalidChecksumError if the checksum does not match.
        nodefilter : callable, optional
            The callable is used to select which files of each product will be downloaded.
            If None (the default), the full products will be downloaded.
            See :mod:`sentinelsat.products` for sample node filters.

        Returns
        -------
        product_info : dict
            Dictionary containing the product's info from get_product_odata() as well as
            the path on disk.

        Raises
        ------
        InvalidChecksumError
            If the MD5 checksum does not match the checksum on the server.
        LTATriggered
            If the product has been archived and its retrieval was successfully triggered.
        LTAError
            If the product has been archived and its retrieval failed.
        """
        downloader = copy(self.downloader)
        downloader.node_filter = nodefilter
        downloader.verify_checksum = checksum
        return downloader.download(id, directory_path,proxy)

    def _get_filename(self, product_info):
        filename = product_info['Name']
        # This should cover all currently existing file types: .SAFE, .SEN3, .nc and .EOF
        filename = filename.replace(".SAFE", ".zip")
        filename = filename.replace(".SEN3", ".zip")
        return filename

    def download_all(
        self,
        products,
        directory_path=".",
        max_attempts=10,
        checksum=True,
        n_concurrent_dl=None,
        lta_retry_delay=None,
        fail_fast=False,
        nodefilter=None,
        proxies=None
    ):
        """Download a list of products.

        Takes a list of product IDs as input. This means that the return value of query() can be
        passed directly to this method.

        File names on the server are used for the downloaded files, e.g.
        "S1A_EW_GRDH_1SDH_20141003T003840_20141003T003920_002658_002F54_4DD1.zip".

        In case of interruptions or other exceptions, downloading will restart from where it left
        off. Downloading is attempted at most max_attempts times to avoid getting stuck with
        unrecoverable errors.

        Parameters
        ----------
        products : list
            List of product IDs
        directory_path : string
            Directory where the downloaded files will be downloaded
        max_attempts : int, default 10
            Number of allowed retries before giving up downloading a product.
        checksum : bool, default True
            If True, verify the downloaded files' integrity by checking its MD5 checksum.
            Throws InvalidChecksumError if the checksum does not match.
            Defaults to True.
        n_concurrent_dl : integer, optional
            Number of concurrent downloads. Defaults to :attr:`SentinelAPI.concurrent_dl_limit`.
        lta_retry_delay : float, default 60
            Number of seconds to wait between requests to the Long Term Archive.
        fail_fast : bool, default False
            if True, all other downloads are cancelled when one of the downloads fails.
        nodefilter : callable, optional
            The callable is used to select which files of each product will be downloaded.
            If None (the default), the full products will be downloaded.
            See :mod:`sentinelsat.products` for sample node filters.

        Notes
        -----
        By default, raises the most recent downloading exception if all downloads failed.
        If ``fail_fast`` is set to True, raises the encountered exception on the first failed
        download instead.

        Returns
        -------
        dict[string, dict]
            A dictionary containing the return value from download() for each successfully
            downloaded product.
        dict[string, dict]
            A dictionary containing the product information for products successfully
            triggered for retrieval from the long term archive but not downloaded.
        dict[string, dict]
            A dictionary containing the product information of products where either
            downloading or triggering failed. "exception" field with the exception info
            is included to the product info dict.
        """
        downloader = copy(self.downloader)
        downloader.verify_checksum = checksum
        downloader.fail_fast = fail_fast
        downloader.max_attempts = max_attempts
        if n_concurrent_dl:
            downloader.n_concurrent_dl = n_concurrent_dl
        if lta_retry_delay:
            downloader.lta_retry_delay = lta_retry_delay
        downloader.node_filter = nodefilter
        statuses, exceptions, product_infos = downloader.download_all(products, directory_path,proxies)

        # Adapt results to the old download_all() API
        downloaded_prods = {}
        started = {}
        failed_prods = {}
        for pid, status in statuses.items():
            if pid not in product_infos:
                product_infos[pid] = {}
            if pid in exceptions:
                product_infos[pid]["exception"] = exceptions[pid]
            if status == DownloadStatus.DOWNLOADED:
                downloaded_prods[pid] = product_infos[pid]
            elif status == DownloadStatus.DOWNLOAD_STARTED:
                started[pid] = product_infos[pid]
            else:
                failed_prods[pid] = product_infos[pid]
        ResultTuple = namedtuple("ResultTuple", ["downloaded", "started", "failed"])
        return ResultTuple(downloaded_prods, started, failed_prods)

    @staticmethod
    def get_products_size(products):
        """Return the total file size in GB of all products in the OpenSearch response."""
        size_total = 0
        for title, props in products.items():
            size_product = props["size"]
            size_value = float(size_product.split(" ")[0])
            size_unit = str(size_product.split(" ")[1])
            if size_unit == "MB":
                size_value /= 1024.0
            if size_unit == "KB":
                size_value /= 1024.0 * 1024.0
            size_total += size_value
        return round(size_total, 2)

    def check_files(self, paths=None, ids=None, directory=None, delete=False):
        """Verify the integrity of product files on disk.

        Integrity is checked by comparing the size and checksum of the file with the respective
        values on the server.

        The input can be a list of products to check or a list of IDs and a directory.

        In cases where multiple products with different IDs exist on the server for given product
        name, the file is considered to be correct if any of them matches the file size and
        checksum. A warning is logged in such situations.

        The corrupt products' OData info is included in the return value to make it easier to
        re-download the products, if necessary.

        Parameters
        ----------
        paths : list[string]
            List of product file paths.
        ids : list[string]
            List of product IDs.
        directory : string
            Directory where the files are located, if checking based on product IDs.
        delete : bool
            Whether to delete corrupt products. Defaults to False.

        Returns
        -------
        dict[str, list[dict]]
            A dictionary listing the invalid or missing files. The dictionary maps the corrupt
            file paths to a list of OData dictionaries of matching products on the server (as
            returned by :meth:`SentinelAPI.get_product_odata()`).
        """
        if not ids and not paths:
            raise ValueError("Must provide either file paths or product IDs and a directory")
        if ids and not directory:
            raise ValueError("Directory value missing")
        if directory is not None:
            directory = Path(directory)
        paths = [Path(p) for p in paths] if paths else []
        ids = ids or []

        # Get product IDs corresponding to the files on disk
        names = []
        if paths:
            names = [p.stem for p in paths]
            result = self._query_names(names)
            for product_dicts in result.values():
                ids += list(product_dicts)
        names_from_paths = set(names)
        ids = set(ids)

        # Collect the OData information for each product
        # Product name -> list of matching odata dicts
        product_infos = defaultdict(list)
        for id in ids:
            odata = self.get_product_odata(id)
            name = odata["title"]
            product_infos[name].append(odata)

            # Collect
            if name not in names_from_paths:
                paths.append(directory / self._get_filename(odata))

        # Now go over the list of products and check them
        corrupt = {}
        for path in paths:
            name = path.stem

            if len(product_infos[name]) > 1:
                self.logger.warning("%s matches multiple products on server", path)

            if not path.exists():
                # We will consider missing files as corrupt also
                self.logger.info("%s does not exist on disk", path)
                corrupt[str(path)] = product_infos[name]
                continue

            is_fine = False
            for product_info in product_infos[name]:
                if path.stat().st_size == product_info["size"] and self._checksum_compare(
                    path, product_info
                ):
                    is_fine = True
                    break
            if not is_fine:
                self.logger.info("%s is corrupt", path)
                corrupt[str(path)] = product_infos[name]
                if delete:
                    path.unlink()

        return corrupt

    def _checksum_compare(self, file_path, product_info, block_size=2**13):
        """Compare a given MD5 checksum with one calculated from a file."""
        checksum = None
        algo = None
        if "Checksum" in product_info:
            checksum_list = product_info['Checksum']
            if len(checksum_list) >0:
                for checksum_dict in checksum_list:
                    if "Algorithm" in checksum_dict:
                        algo = checksum_dict['Algorithm']
                        if algo == "sha3-256":
                            checksum = checksum_dict['Value']
                            algo = hashlib.sha3_256() 
                        elif algo == "MD5":
                            checksum = checksum_dict['Value']
                            algo = hashlib.md5()
                            break
                        elif algo == "BLAKE3":
                            checksum = checksum_dict['Value']
                            algo = hashlib.blake2b() # TODO ?
                    else:
                        raise InvalidChecksumError("No checksum information found in product information. No Algorithm provided provided")
            else:
                        raise InvalidChecksumError("No checksum information found in product information. The Checksum list is empty")
        if checksum == None:
            raise InvalidChecksumError("No checksum algo provided is supported.")
        # input(f"checksum found {algo} {checksum}...")
        file_path = Path(file_path)
        file_size = file_path.stat().st_size
        with self._tqdm(
            desc=f"{algo.name.upper()} checksumming",
            total=file_size,
            unit="B",
            unit_scale=True,
            leave=False,
        ) as progress:
            with open(file_path, "rb") as f:
                while True:
                    block_data = f.read(block_size)
                    if not block_data:
                        break
                    algo.update(block_data)
                    progress.update(len(block_data))
            return algo.hexdigest().lower() == checksum.lower()

    def _tqdm(self, **kwargs):
        """tqdm progressbar wrapper. May be overridden to customize progressbar behavior"""
        kwargs.update({"disable": not self.show_progressbars})
        return tqdm(**kwargs)

    def get_stream(self, id, **kwargs):
        """Exposes requests response ready to stream product to e.g. S3.

        Parameters
        ----------
        id : string
            UUID of the product, e.g. 'a8dd0cfd-613e-45ce-868c-d79177b916ed'
        **kwargs
            Any additional parameters for :func:`requests.get()`

        Raises
        ------
        LTATriggered
            If the product has been archived and its retrieval was successfully triggered.
        LTAError
            If the product has been archived and its retrieval failed.

        Returns
        -------
        requests.Response:
            Opened response object
        """
        return self.downloader.get_stream(id, **kwargs)

    def _get_odata_url(self, uuid, suffix=""):
        return "https://catalogue.dataspace.copernicus.eu/" + f"odata/v1/Products({uuid})" + suffix

    def _get_download_url(self, uuid):
        return self._get_odata_url(uuid, "/$value")

def read_collections(collection):
    retsult = []
    word = ""
    level = -1
    for i in collection:
        if i== "(":
            if level ==-1:
                level = 1
            else:
                level+=1
        if i == ")":
            level-=1
        word+= i
        if level == 0:
            level = -1
            retsult.append(word)
            word = ""
    return retsult
        

def remove_geometry_collection(wkt: str):
    geometry_collection = "GEOMETRYCOLLECTION(" 
    index = wkt.find(geometry_collection)
    if index>=0:
        wkt_ = wkt[len(geometry_collection):len(wkt)-1]
        return wkt_
    else:
        return wkt

def read_geojson(geojson_file):
    """Read a GeoJSON file into a GeoJSON object."""
    with open(geojson_file) as f:
        return geojson.load(f)


def geojson_to_wkt(geojson_obj, decimals=4):
    """Convert a GeoJSON object to Well-Known Text. Intended for use with OpenSearch queries.
    3D points are converted to 2D.

    Parameters
    ----------
    geojson_obj : dict
        a GeoJSON object
    decimals : int, optional
        Number of decimal figures after point to round coordinate to. Defaults to 4 (about 10
        meters).

    Returns
    -------
    str
        Well-Known Text string representation of the geometry
    """
    if "coordinates" in geojson_obj:
        geometry = geojson_obj
    elif "geometry" in geojson_obj:
        geometry = geojson_obj["geometry"]
    else:
        geometry = {"type": "GeometryCollection", "geometries": []}
        for feature in geojson_obj["features"]:
            geometry["geometries"].append(feature["geometry"])

    def ensure_2d(geometry):
        if isinstance(geometry[0], (list, tuple)):
            return list(map(ensure_2d, geometry))
        else:
            return geometry[:2]

    def check_bounds(geometry):
        if isinstance(geometry[0], (list, tuple)):
            return list(map(check_bounds, geometry))
        else:
            if geometry[0] > 180 or geometry[0] < -180:
                raise ValueError("Longitude is out of bounds, check your JSON format or data")
            if geometry[1] > 90 or geometry[1] < -90:
                raise ValueError("Latitude is out of bounds, check your JSON format or data")

    # Discard z-coordinate, if it exists
    if geometry["type"] == "GeometryCollection":
        for idx, geo in enumerate(geometry["geometries"]):
            geometry["geometries"][idx]["coordinates"] = ensure_2d(geo["coordinates"])
            check_bounds(geo["coordinates"])
    else:
        geometry["coordinates"] = ensure_2d(geometry["coordinates"])
        check_bounds(geometry["coordinates"])

    wkt = geomet.wkt.dumps(geometry, decimals=decimals)
    # Strip unnecessary spaces
    wkt = re.sub(r"(?<!\d) ", "", wkt)
    return wkt

def format_query_date(in_date):
    r"""
    Format a date, datetime or a YYYYMMDD string input as YYYY-MM-DDThh:mm:ssZ
    or validate a date string as suitable for the full text search interface and return it.

    `None` will be converted to '\*', meaning an unlimited date bound in date ranges.

    Parameters
    ----------
    in_date : str or datetime or date or None
        Date to be formatted

    Returns
    -------
    str
        Formatted string

    Raises
    ------
    ValueError
        If the input date type is incorrect or passed date string is invalid
    """
    if in_date is None:
        return "*"
    if isinstance(in_date, (datetime, date)):
        return in_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    elif not isinstance(in_date, str):
        raise ValueError("Expected a string or a datetime object. Received {}.".format(in_date))

    in_date = in_date.strip()
    if in_date == "*":
        # '*' can be used for one-sided range queries e.g. ingestiondate:[* TO NOW-1YEAR]
        return in_date

    # Reference: https://cwiki.apache.org/confluence/display/solr/Working+with+Dates

    # ISO-8601 date or NOW
    valid_date_pattern = r"^(?:\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z|NOW)"
    # date arithmetic suffix is allowed
    units = r"(?:YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)"
    valid_date_pattern += r"(?:[-+]\d+{}S?)*".format(units)
    # dates can be rounded to a unit of time
    # e.g. "NOW/DAY" for dates since 00:00 today
    valid_date_pattern += r"(?:/{}S?)*$".format(units)
    in_date = in_date.strip()
    if re.match(valid_date_pattern, in_date):
        return in_date

    try:
        return datetime.strptime(in_date, "%Y%m%d").strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        raise ValueError("Unsupported date value {}".format(in_date))

def placename_to_wkt(place_name):
    """Geocodes the place name to rectangular bounding extents using Nominatim API and
       returns the corresponding 'ENVELOPE' form Well-Known-Text.

    Parameters
    ----------
    place_name : str
        the query to geocode

    Raises
    ------
    ValueError
        If no matches to the place name were found.

    Returns
    -------
    wkt_envelope : str
        Bounding box of the location as an 'ENVELOPE(minX, maxX, maxY, minY)' WKT string.
    info : Dict[str, any]
        Matched location's metadata returned by Nominatim.
    """
    rqst = requests.get(
        "https://nominatim.openstreetmap.org/search",
        params={"q": place_name, "format": "geojson"},
        headers={"User-Agent": "sentinelsat/" + sentinelsat_version},
    )
    rqst.raise_for_status()
    features = rqst.json()["features"]
    if len(features) == 0:
        raise ValueError('Unable to find a matching location for "{}"'.format(place_name))
    # Get the First result's bounding box and description.
    feature = features[0]
    minX, minY, maxX, maxY = feature["bbox"]
    # ENVELOPE is a non-standard WKT format supported by Solr
    # https://lucene.apache.org/solr/guide/6_6/spatial-search.html#SpatialSearch-BBoxField
    wkt_envelope = "ENVELOPE({}, {}, {}, {})".format(minX, maxX, maxY, minY)
    info = feature["properties"]
    info["bbox"] = feature["bbox"]
    return wkt_envelope, info


def is_wkt(possible_wkt):
    pattern = r"^((MULTI)?(POINT|LINESTRING|POLYGON)|GEOMETRYCOLLECTION|ENVELOPE)\s*\(.+\)$"
    return re.match(pattern, possible_wkt.strip().upper()) is not None
