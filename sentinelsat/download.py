import concurrent.futures
import enum
import itertools
import shutil
import threading
import time
import traceback
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from typing import Any, Dict
from xml.etree import ElementTree as etree

from sentinelsat.exceptions import (
    InvalidChecksumError,
    SentinelAPIError,
    ServerError,
    UnauthorizedError,
)


class DownloadStatus(enum.Enum):
    """Status info for :meth:`Downloader.download_all()`.

    Evaluates to True if status is :obj:`DOWNLOADED`.
    """

    UNAVAILABLE = enum.auto()
    OFFLINE = enum.auto()
    TRIGGERED = enum.auto()
    ONLINE = enum.auto()
    DOWNLOAD_STARTED = enum.auto()
    DOWNLOADED = enum.auto()

    def __bool__(self):
        return self == DownloadStatus.DOWNLOADED


class Downloader:
    """
    Manages downloading of products or parts of them.

    Intended for internal use, but may also be used directly if more fine-grained
    configuration or custom download logic is needed.

    Parameters
    ----------
    api : SentinelAPI
        A SentinelAPI instance.
    node_filter : callable, optional
        The callable is used to select which files of each product will be downloaded.
        If None (the default), the full products will be downloaded.
        See :mod:`sentinelsat.products` for sample node filters.
    verify_checksum : bool, default True
        If True, verify the downloaded files' integrity by checking its checksum.
        Throws InvalidChecksumError if the checksum does not match.
    fail_fast : bool, default False
        if True, all other downloads are cancelled when one of the downloads fails in :meth:`download_all()`.
    n_concurrent_dl : integer, optional
        Number of concurrent downloads.
        Defaults to the maximum allowed by :attr:`SentinelAPI.concurrent_dl_limit`.
    max_attempts : int, default 10
        Number of allowed retries before giving up downloading a product in :meth:`download_all()`.
    dl_retry_delay : float, default 10
        Number of seconds to wait between retrying of failed downloads.
    lta_retry_delay : float, default 60
        Number of seconds to wait between requests to the Long Term Archive.
    lta_timeout : float, optional
        Maximum number of seconds to wait for triggered products to come online.
        Defaults to no timeout.
    """

    def __init__(
        self,
        api,
        *,
        node_filter=None,
        verify_checksum=True,
        fail_fast=False,
        n_concurrent_dl=None,
        max_attempts=10,
        dl_retry_delay=10,
        lta_retry_delay=60,
        lta_timeout=None
    ):
        from sentinelsat import SentinelAPI

        self.api: SentinelAPI = api
        self.logger = self.api.logger
        self._tqdm = self.api._tqdm

        self.node_filter = node_filter
        self.verify_checksum = verify_checksum
        self.fail_fast = fail_fast
        self.max_attempts = max_attempts
        self.n_concurrent_dl = n_concurrent_dl or self.api.concurrent_dl_limit
        self.dl_retry_delay = dl_retry_delay
        self.lta_retry_delay = lta_retry_delay
        self.lta_timeout = lta_timeout
        self.chunk_size = 2**20  # download in 1 MB chunks by default

    def download(self, id, directory=".", *, stop_event=None,proxy=None):
        """Download a product.

        Parameters
        ----------
        id : string
            UUID of the product, e.g. 'a8dd0cfd-613e-45ce-868c-d79177b916ed'
        directory : string or Path, optional
            Where the file will be downloaded
        stop_event : threading.Event, optional
            An event object can be provided and set to interrupt the download.

        Returns
        -------
        product_info : dict
            Dictionary containing the product's info from get_product_odata() as well as
            the path on disk.

        Raises
        ------
        InvalidChecksumError
            If the checksum does not match the checksum on the server.
        LTATriggered
            If the product has been archived and its retrieval was successfully triggered.
        LTAError
            If the product has been archived and its retrieval failed.
        """
        if self.node_filter:
            return self._download_with_node_filter(id, directory, stop_event)

        product_info = self.api.get_product_odata(id)
        # print(product_info)
        filename = self.api._get_filename(product_info)
        path = Path(directory) / filename
        product_info["path"] = str(path)
        product_info["downloaded_bytes"] = 0

        if path.exists():
            # We assume that the product has been downloaded and is complete
            return product_info
        self._download_common(product_info, path, stop_event,proxy)
        return product_info

    def _download_common(self, product_info: Dict[str, Any], path: Path, stop_event,proxy=None):
        # Use a temporary file for downloading

        temp_path = path.with_name(path.name + ".incomplete")
        print(f"DOWNLOADING: {product_info['Name']} with size {product_info['ContentLength']}")
        skip_download = False
        # dont_skip_checksum_and_size_check = False # TODO when the data from the ESA will be working correctly check size and checksum 
        # if dont_skip_checksum_and_size_check:
        #     if temp_path.exists():
        #         size = temp_path.stat().st_size
        #         # print(f"\n\nExists temp path: {temp_path},size: {size}, info_size: {product_info['ContentLength']}\n\n")
        #         # input('\n\nwait for input\n\n')
        #         if size > product_info["ContentLength"]:
        #             self.logger.warning(
        #                 "Existing incomplete file %s is larger than the expected final size"
        #                 " (%s vs %s bytes). Deleting it.",
        #                 str(temp_path),
        #                 size,
        #                 product_info["ContentLength"],
        #             )
        #             temp_path.unlink()
        #             raise(f"Content length mismatch for {path.name}")
        #         elif size == product_info["ContentLength"]:
        #             if self.verify_checksum and not self.api._checksum_compare(temp_path, product_info):
        #                 # Log a warning since this should never happen
        #                 self.logger.warning(
        #                     "Existing incomplete file %s appears to be fully downloaded but "
        #                     "its checksum is incorrect. Deleting it.",
        #                     str(temp_path),
        #                 )
        #                 temp_path.unlink()
        #             else:
        #                 skip_download = True
        #         else:
        #             # continue downloading
        #             self.logger.info(
        #                 "Download will resume from existing incomplete file %s.", temp_path
        #             )
        #             pass
        if not skip_download:
            # Store the number of downloaded bytes for unit tests
            print(f"First time download {temp_path}")
            self.logger.info(f"First time download {temp_path}")
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            pid = product_info['Id']
            url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({pid})/$value"
            product_info["downloaded_bytes"] = self._download(
                url,
                temp_path,
                product_info["ContentLength"],
                path.name,
                stop_event,
                proxy
            )
        size = temp_path.stat().st_size
        if size > product_info["ContentLength"]:
                self.logger.warning(
                    "Existing incomplete file %s is larger than the expected final size"
                    " (%s vs %s bytes). Deleting it.",
                    str(temp_path),
                    size,
                    product_info["ContentLength"],
                )
                temp_path.unlink()
                raise(f"Content length mismatch for {path.name}")
        elif size == product_info["ContentLength"]:
            # Check integrity with MD5 checksum
            if self.verify_checksum:
                # Log a warning since this should never happen
                if self.api._checksum_compare(temp_path, product_info):
                    self.logger.warning(
                        "Existing incomplete file %s appears to be fully downloaded and checksum valid: ",
                        str(temp_path),
                    )
                    shutil.move(temp_path, path)
                else:
                    self.logger.warning(
                        "Existing incomplete file %s appears to be fully downloaded but "
                        "its checksum is incorrect. Deleting it.",
                        str(temp_path),
                    )
                    temp_path.unlink()
                    raise(f"Checksum mismatch for file {path.name}")
        return product_info

    def download_all(self, products_dict, directory=".",proxy=None):
        """Download a list of products.

        Parameters
        ----------
        products : list
            List of product IDs
        directory : string or Path, optional
            Directory where the files will be downloaded

        Notes
        ------
        By default, raises the most recent downloading exception if all downloads failed.
        If :attr:`Downloader.fail_fast` is set to True, raises the encountered exception on the first failed
        download instead.

        Returns
        -------
        dict[string, DownloadStatus]
            The status of all products.
        dict[string, Exception]
            Exception info for any failed products.
        dict[string, dict]
            A dictionary containing the product information for each product
            (unless the product was unavailable).
        """

        ResultTuple = namedtuple("ResultTuple", ["statuses", "exceptions", "product_infos"])
        products = self.api.from_oreded_dict_to_dataframe(products_dict)
        product_ids = list(set(products['Id']))
        print(len(product_ids))
        assert self.n_concurrent_dl > 0
        if len(product_ids) == 0:
            return ResultTuple({}, {}, {})
        self.logger.info(
            "Will download %d products using %d workers", len(product_ids), self.n_concurrent_dl
        )

        statuses = self._init_statuses(
            product_ids,products
        )

        # Skip already downloaded files.
        products = self._skip_existing_products(directory, products, statuses)
        # print(f"statuses: {statuses}")
        # input("...")
        product_ids = list(set(products['Id']))
        stop_event = threading.Event()
        exceptions = {}
        product_infos = {}

        # One threadpool for downloading.
        dl_count = len(product_ids)
        dl_executor = ThreadPoolExecutor(
            max_workers=max(1, min(self.n_concurrent_dl, dl_count)),
            thread_name_prefix="dl",
        )
        dl_progress = self._tqdm(
            total=dl_count,
            desc="Downloading products",
            unit="product",
        )
        try:
            # All online products are downloaded.
            for index,row in products.iterrows():
                future = dl_executor.submit(
                    self._download_online_retry,
                    row['Id'],
                    row['Name'],
                    directory,
                    stop_event,
                    statuses,
                    exceptions,
                    product_infos,
                    proxy
                )
                # dl_tasks[future] = pid
        except Exception as e:
            raise(Exception(f"Error during downloading! {e}"))
        finally:
            dl_executor.shutdown(wait=True)
            dl_progress.close()
        return ResultTuple(statuses, exceptions, product_infos)

    def _init_statuses(self, product_ids,products):
        # print(product_ids)
        statuses = {pid: DownloadStatus.UNAVAILABLE for pid in product_ids}
        for index,row in products.iterrows():
            pid = row['Id']
            if row['Online']:
                statuses[pid] = DownloadStatus.ONLINE
            else:
                statuses[pid] = DownloadStatus.OFFLINE
        return statuses

    def _skip_existing_products(self, directory, products, statuses):
        for index,row in products.iterrows():
            pid = row['Id']
            # filename = row['Name']
            filename = self.api._get_filename(row)
            path = Path(directory) / filename
            ff = filename+".incomplete"
            path_incomplete = Path(directory) /ff
            print(path_incomplete)
            if path.exists():
                self.logger.info("Skipping already downloaded %s.", filename)
                statuses[pid] = DownloadStatus.DOWNLOADED
                products.drop([index])
            if path_incomplete.exists():
                #TODO when metadata working delete this part delete for now the already downloaded part
                # statuses[pid] = DownloadStatus.DOWNLOADED_INCOMPLETE
                import os
                # print(f"Before removal {path_incomplete}...")
                os.remove(path_incomplete)
                 #print(f"After removal {path_incomplete}...")
                if path.exists():
                    # print(f"Before removal {path}...")
                    os.remove(path)
                    # print(f"After removal {path}...")
                print(f"file: {path_incomplete} deleted!")
        return products
    
    def _download_online_retry_new(self, pid: str,name: str, directory,stop_event: threading.Event,statuses):
        print(f"HEREEEEEEEEEEEEEEEEEEEEE {pid}")
        self.download(pid,directory,stop_event)
        print("caso")

    def _download_online_retry(self, pid: str,name: str, directory,stop_event: threading.Event,statuses,exceptions,product_infos,proxy=None):
        """Thin wrapper around download with retrying and checking whether a product is online

        Parameters
        ----------
        product_info : str
        name: str
        directory : string, optional
        stop_event : threading.Event
        statuses : dict of DownloadStatus
        """
        if self.max_attempts <= 0:
            return

        uuid = pid
        title = name

        # Wait for the triggering and retrieval to complete first
        print("1",stop_event.is_set(),self.max_attempts)
        while (stop_event.is_set()):
            print("Waiting")
            _wait(stop_event, 1)
        last_exception = None
        for cnt in range(self.max_attempts):
            last_exception = None
            if stop_event.is_set():
                raise concurrent.futures.CancelledError()
            try:
                if cnt > 0:
                    _wait(stop_event, self.dl_retry_delay)
                statuses[uuid] = DownloadStatus.DOWNLOAD_STARTED
                product_info = self.download(uuid, directory, stop_event=stop_event,proxy=proxy)
                product_infos[uuid] = product_info
                statuses[uuid] = DownloadStatus.DOWNLOADED
                return product_info
            except (concurrent.futures.CancelledError, KeyboardInterrupt, SystemExit):
                raise Exception("THE DOWNLOAD WAS CANCELED!")
            except Exception as e:
                if isinstance(e, InvalidChecksumError):
                    self.logger.warning(
                        "Invalid checksum. The downloaded file for '%s' is corrupted.",
                        title,
                    )
                else:
                    self.logger.exception("There was an error downloading %s", title)
                retries_remaining = self.max_attempts - cnt - 1
                if retries_remaining > 0:
                    self.logger.info(
                        "%d retries left, retrying in %s seconds...",
                        retries_remaining,
                        self.dl_retry_delay,
                    )
                    #TODO remove this part when metadata working with partial files
                    ff = name.replace(".SAFE",".zip")+".incomplete"
                    path_incomplete = Path(directory) /ff
                    print(path_incomplete)
                    import os
                    os.remove(path_incomplete)
                else:
                    self.logger.info("Downloading %s failed. No retries left.", title)
                last_exception = e
                exceptions[uuid] = e
                continue

        if last_exception is not None:
            raise last_exception

    def _download(self, url, path, file_size, title, stop_event,proxy= None):
        headers = {}
        continuing = path.exists()
        self.api.if_token_expired_refresh()
        access_token = self.api.token
        if continuing:
            already_downloaded_bytes = 0#path.stat().st_size #TODO remove this 0 #path.stat().st_size
            print(url)
            headers = {"Authorization": f"Bearer {access_token}","Range": "bytes={}-".format(already_downloaded_bytes)}
            # input("????????????????????????????????????????????")
        else:
            already_downloaded_bytes = 0
            headers = {"Authorization": f"Bearer {access_token}"}
        downloaded_bytes = already_downloaded_bytes
        # self.api.session.get(url, stream=True, headers=headers)
        with self.api.dl_limit_semaphore:
            print("GETTING DOWNLOAD",self.api.dl_limit_semaphore._value)
            import requests
            if proxy != None:
                print(f"Using proxies: {proxy}")
                input("...")
                r = requests.get(url, stream=True, headers=headers, proxies=proxy)
            else:
                r = requests.get(url, stream=True, headers=headers)
        with self._tqdm(
            desc=f"Downloading {title}",
            total=file_size,
            unit="B",
            unit_scale=True,
            initial=already_downloaded_bytes,
        ) as progress, closing(r):
            # self.api._check_scihub_response(r, test_json=False)
            mode = "ab" if continuing else "wb"
            with open(path, mode) as f:
                for chunk in r.iter_content(chunk_size=self.chunk_size):
                    if stop_event and stop_event.is_set():
                        raise concurrent.futures.CancelledError()
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        progress.update(len(chunk))
                        downloaded_bytes += len(chunk)
        return downloaded_bytes

    def _dataobj_to_node_info(self, dataobj_info, product_info):
        path = dataobj_info["href"]
        if path.startswith("./"):
            path = path[2:]

        node_info = product_info.copy()
        node_info["url"] = self.api._path_to_url(product_info, path, "value")
        node_info["size"] = dataobj_info["size"]
        if "md5" in dataobj_info:
            node_info["md5"] = dataobj_info["md5"]
        if "sha3-256" in dataobj_info:
            node_info["sha3-256"] = dataobj_info["sha3-256"]
        node_info["node_path"] = path
        # node_info["parent"] = product_info

        return node_info

    def _filter_nodes(self, manifest, product_info, nodefilter=None):
        nodes = {}
        xmldoc = etree.parse(manifest)
        data_obj_section_elem = xmldoc.find("dataObjectSection")
        for elem in data_obj_section_elem.iterfind("dataObject"):
            dataobj_info = _xml_to_dataobj_info(elem)
            node_info = self._dataobj_to_node_info(dataobj_info, product_info)
            if nodefilter is not None and not nodefilter(node_info):
                continue
            node_path = node_info["node_path"]
            nodes[node_path] = node_info
        return nodes


def _xml_to_dataobj_info(element):
    assert etree.iselement(element)
    assert element.tag == "dataObject"
    data = dict(
        id=element.attrib["ID"],
    )
    elem = element.find("byteStream")
    # data["mime_type"] = elem.attrib['mimeType']
    data["size"] = int(elem.attrib["size"])
    elem = element.find("byteStream/fileLocation")
    data["href"] = elem.attrib["href"]
    # data['locator_type'] = elem.attrib["locatorType"]
    # assert data['locator_type'] == "URL"

    elem = element.find("byteStream/checksum")
    assert elem.attrib["checksumName"].upper() in ["MD5", "SHA3-256"]
    data[elem.attrib["checksumName"].lower()] = elem.text

    return data


def _format_exception(ex):
    return "".join(traceback.TracebackException.from_exception(ex).format())


def _wait(event, timeout):
    """Wraps event.wait so it can be disabled for testing."""
    return event.wait(timeout)
