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

    def download(self, id, directory=".", *, stop_event=None):
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
        self._download_common(product_info, path, stop_event)
        return product_info

    # def _download_with_node_filter(self, id, directory, stop_event):
    #     product_info = self.api.get_product_odata(id)
    #     product_path = Path(directory) / (product_info["product_root_dir"])
    #     product_info["node_path"] = "./" + product_info["product_root_dir"]
    #     manifest_path = product_path / product_info["manifest_name"]
    #     if not manifest_path.exists() and self.trigger_offline_retrieval(id):
    #         raise LTATriggered(id)
    #     manifest_info, _ = self.api._get_manifest(product_info, manifest_path)
    #     product_info["nodes"] = {
    #         manifest_info["node_path"]: manifest_info,
    #     }
    #     node_infos = self._filter_nodes(manifest_path, product_info, self.node_filter)
    #     product_info["nodes"].update(node_infos)
    #     for node_info in node_infos.values():
    #         if stop_event and stop_event.is_set():
    #             raise concurrent.futures.CancelledError()
    #         node_path = node_info["node_path"]
    #         path = (product_path / node_path).resolve()
    #         node_info["path"] = path
    #         node_info["downloaded_bytes"] = 0

    #         self.logger.debug("Downloading %s node to %s", id, path)
    #         self.logger.debug("Node URL for %s: %s", id, node_info["url"])

    #         if path.exists():
    #             # We assume that the product node has been downloaded and is complete
    #             continue
    #         self._download_common(node_info, path, stop_event)
    #     return product_info

    def _download_common(self, product_info: Dict[str, Any], path: Path, stop_event):
        # Use a temporary file for downloading

        temp_path = path.with_name(path.name + ".incomplete")
        skip_download = False
        dont_skip_checksum_and_size_check = False # TODO when the data from the ESA will be working correctly check size and checksum 
        if dont_skip_checksum_and_size_check:
            if temp_path.exists():
                size = temp_path.stat().st_size
                print(f"\n\nExists temp path: {temp_path},size: {size}, info_size: {product_info['ContentLength']}\n\n")
                input('\n\nwait for input\n\n')
                if size > product_info["ContentLength"]:
                    self.logger.warning(
                        "Existing incomplete file %s is larger than the expected final size"
                        " (%s vs %s bytes). Deleting it.",
                        str(temp_path),
                        size,
                        product_info["ContentLength"],
                    )
                    temp_path.unlink()
                elif size == product_info["ContentLength"]:
                    if self.verify_checksum and not self.api._checksum_compare(temp_path, product_info):
                        # Log a warning since this should never happen
                        self.logger.warning(
                            "Existing incomplete file %s appears to be fully downloaded but "
                            "its checksum is incorrect. Deleting it.",
                            str(temp_path),
                        )
                        temp_path.unlink()
                    else:
                        skip_download = True
                else:
                    # continue downloading
                    self.logger.info(
                        "Download will resume from existing incomplete file %s.", temp_path
                    )
                    pass
        if not skip_download:
            # Store the number of downloaded bytes for unit tests
            print(f"First time download {temp_path}")
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            pid = product_info['Id']
            url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({pid})/$value"
            product_info["downloaded_bytes"] = self._download(
                url,
                temp_path,
                product_info["ContentLength"],
                path.name,
                stop_event,
            )
        # Check integrity with MD5 checksum
        if dont_skip_checksum_and_size_check:
            if self.verify_checksum is True:
                if not self.api._checksum_compare(temp_path, product_info):
                    temp_path.unlink()
                    raise InvalidChecksumError("File corrupt: checksums do not match")
        # Download successful, rename the temporary file to its proper name
        shutil.move(temp_path, path)
        return product_info

    def download_all(self, products_dict, directory="."):
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
        product_ids = list(set(products['Id']))
        stop_event = threading.Event()
        dl_tasks = {}
        trigger_tasks = {}

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
                    statuses
                )
                # dl_tasks[future] = pid
        except Exception as e:
            raise(Exception(f"Error during downloading! {e}"))
        return ResultTuple(statuses, {}, {})
        #     for task in concurrent.futures.as_completed(list(trigger_tasks) + list(dl_tasks)):
        #         pid = trigger_tasks.get(task) or dl_tasks[task]
        #         exception = exceptions.get(pid)
        #         if task.cancelled():
        #             exception = concurrent.futures.CancelledError()
        #         if task.exception():
        #             exception = task.exception()

        #         if task in dl_tasks:
        #             if not exception:
        #                 product_infos[pid] = task.result()
        #                 statuses[pid] = DownloadStatus.DOWNLOADED
        #             dl_progress.update()
        #             # Keep the LTA progress fresh
        #             if offline_prods:
        #                 trigger_progress.update(0)
        #         else:
        #             trigger_progress.update()
        #             if all(t.done() for t in trigger_tasks):
        #                 trigger_progress.close()

        #         if exception:
        #             exceptions[pid] = exception
        #             if self.fail_fast:
        #                 raise exception from None
        #             else:
        #                 self.logger.error("%s failed: %s", pid, _format_exception(exception))
        # except:
        #     stop_event.set()
        #     for t in list(trigger_tasks) + list(dl_tasks):
        #         t.cancel()
        #     raise
        # finally:
        #     dl_executor.shutdown()
        #     dl_progress.close()
        #     if offline_prods:
        #         trigger_executor.shutdown()
        #         trigger_progress.close()

        # if not any(statuses):
        #     if not exceptions:
        #         raise SentinelAPIError("Downloading all products failed for an unknown reason")
        #     exception = list(exceptions)[0]
        #     raise exception

        # # Update Online status in product_infos
        # for pid, status in statuses.items():
        #     if status in [DownloadStatus.OFFLINE, DownloadStatus.TRIGGERED]:
        #         product_infos[pid]["Online"] = False
        #     elif status != DownloadStatus.UNAVAILABLE:
        #         product_infos[pid]["Online"] = True

        # return ResultTuple(statuses, exceptions, product_infos)

    def _init_statuses(self, product_ids,products):
        # print(product_ids)
        statuses = {pid: DownloadStatus.UNAVAILABLE for pid in product_ids}
        for index,row in products.iterrows():
            pid = row['Id']
            if row['Online'] == 'Online':
                statuses[pid] = DownloadStatus.ONLINE
            else:
                statuses[pid] = DownloadStatus.OFFLINE
        return statuses

    def _skip_existing_products(self, directory, products, statuses):
        for index,row in products.iterrows():
            pid = row['Id']
            filename = row['Name']
            path = Path(directory) / filename
            if path.exists():
                self.logger.info("Skipping already downloaded %s.", filename)
                statuses[pid] = DownloadStatus.DOWNLOADED
                products.drop([index])
        return products     

    # def trigger_offline_retrieval(self, uuid):
    #     """Triggers retrieval of an offline product.

    #     Parameters
    #     ----------
    #     uuid : string
    #         UUID of the product

    #     Returns
    #     -------
    #     bool
    #         True, if the product retrieval was successfully triggered.
    #         False, if the product is already online.

    #     Raises
    #     ------
    #     LTAError
    #         If the request was not accepted due to exceeded user quota or server overload.
    #     ServerError
    #         If an unexpected response was received from server.
    #     UnauthorizedError
    #         If the provided credentials were invalid.

    #     Notes
    #     -----
    #     https://scihub.copernicus.eu/userguide/LongTermArchive
    #     """
    #     # Request just a single byte to avoid accidental downloading of the whole product.
    #     # Requesting zero bytes results in NullPointerException in the server.
    #     with self.api.dl_limit_semaphore:
    #         r = self.api.session.get(
    #             self.api._get_download_url(uuid), headers={"Range": "bytes=0-1"}
    #         )
    #     cause = r.headers.get("cause-message")
    #     # check https://scihub.copernicus.eu/userguide/LongTermArchive#HTTP_Status_codes
    #     if r.status_code in (200, 206):
    #         self.logger.debug("Product is online")
    #         return False
    #     elif r.status_code == 202:
    #         self.logger.debug("Accepted for retrieval")
    #         return True
    #     elif r.status_code == 403 and cause and "concurrent flows" in cause:
    #         # cause: 'An exception occured while creating a stream: Maximum number of 4 concurrent flows achieved by the user "username""'
    #         self.logger.debug("Product is online but concurrent downloads limit was exceeded")
    #         return False
    #     elif r.status_code == 403:
    #         # cause: 'User 'username' offline products retrieval quota exceeded (20 fetches max) trying to fetch product PRODUCT_FILENAME (BYTES_COUNT bytes compressed)'
    #         msg = f"User quota exceeded: {cause}"
    #         self.logger.error(msg)
    #         raise LTAError(msg, r)
    #     elif r.status_code == 503:
    #         msg = f"Request not accepted: {cause}"
    #         self.logger.error(msg)
    #         raise LTAError(msg, r)
    #     elif r.status_code < 400:
    #         msg = f"Unexpected response {r.status_code}: {cause}"
    #         self.logger.error(msg)
    #         raise ServerError(msg, r)
    #     self.api._check_scihub_response(r, test_json=False)

    # def get_stream(self, id, **kwargs):
    #     """Exposes requests response ready to stream product to e.g. S3.

    #     Parameters
    #     ----------
    #     id : string
    #         UUID of the product, e.g. 'a8dd0cfd-613e-45ce-868c-d79177b916ed'
    #     **kwargs
    #         Any additional parameters for :func:`requests.get()`

    #     Raises
    #     ------
    #     LTATriggered
    #         If the product has been archived and its retrieval was successfully triggered.
    #     LTAError
    #         If the product has been archived and its retrieval failed.

    #     Returns
    #     -------
    #     requests.Response:
    #         Opened response object
    #     """
    #     if not self.api.is_online(id):
    #         self.trigger_offline_retrieval(id)
    #         raise LTATriggered(id)
    #     with self.api.dl_limit_semaphore:
    #         r = self.api.session.get(self.api._get_download_url(id), stream=True, **kwargs)
    #     self.api._check_scihub_response(r, test_json=False)
    #     return r

    # def download_quicklook(self, id, directory="."):
    #     """Download a quicklook for a product.

    #     Parameters
    #     ----------
    #     id : string
    #         UUID of the product, e.g. 'a8dd0cfd-613e-45ce-868c-d79177b916ed'
    #     directory : string or Path, optional
    #         Where the image will be downloaded. Defaults to ".".

    #     Returns
    #     -------
    #     quicklook_info : dict
    #         Dictionary containing the quicklooks's response headers as well as the path on disk.
    #     """
    #     product_info = self.api.get_product_odata(id)
    #     url = product_info["quicklook_url"]

    #     path = Path(directory) / "{}.jpeg".format(product_info["title"])
    #     product_info["path"] = str(path)
    #     product_info["downloaded_bytes"] = 0
    #     product_info["error"] = ""

    #     self.logger.info("Downloading quicklook %s to %s", product_info["title"], path)

    #     with self.api.dl_limit_semaphore:
    #         r = self.api.session.get(url)
    #     self.api._check_scihub_response(r, test_json=False)

    #     product_info["quicklook_size"] = len(r.content)

    #     if path.exists():
    #         return product_info

    #     content_type = r.headers["content-type"]
    #     if content_type != "image/jpeg":
    #         product_info["error"] = "Quicklook is not jpeg but {}".format(content_type)

    #     if product_info["error"] == "":
    #         with open(path, "wb") as fp:
    #             fp.write(r.content)
    #             product_info["downloaded_bytes"] = len(r.content)

    #     return product_info

    # def download_all_quicklooks(self, products, directory="."):
    #     """Download quicklook for a list of products.

    #     Parameters
    #     ----------
    #     products : dict
    #         Dict of product IDs, product data
    #     directory : string or Path, optional
    #         Directory where the downloaded images will be downloaded
    #     n_concurrent_dl : integer
    #         Number of concurrent downloads

    #     Returns
    #     -------
    #     dict[string, dict]
    #         A dictionary containing the return value from download_quicklook() for each
    #         successfully downloaded quicklook
    #     dict[string, dict]
    #         A dictionary containing the error of products where either
    #         quicklook was not available or it had an unexpected content type
    #     """

    #     self.logger.info("Will download %d quicklooks", len(products))

    #     downloaded_quicklooks = {}
    #     failed_quicklooks = {}

    #     with ThreadPoolExecutor(max_workers=self.n_concurrent_dl) as dl_exec:
    #         dl_tasks = {}
    #         for pid in products:
    #             future = dl_exec.submit(self.download_quicklook, pid, directory)
    #             dl_tasks[future] = pid

    #         completed_tasks = concurrent.futures.as_completed(dl_tasks)

    #         for future in completed_tasks:
    #             product_info = future.result()
    #             if product_info["error"] == "":
    #                 downloaded_quicklooks[dl_tasks[future]] = product_info
    #             else:
    #                 failed_quicklooks[dl_tasks[future]] = product_info["error"]

    #     ResultTuple = namedtuple("ResultTuple", ["downloaded", "failed"])
    #     return ResultTuple(downloaded_quicklooks, failed_quicklooks)

    # def _trigger_and_wait(self, uuid, stop_event, statuses):
    #     """Continuously triggers retrieval of offline products

    #     This function is supposed to be called in a separate thread. By setting stop_event it can be stopped.
    #     """
    #     with self.api.lta_limit_semaphore:
    #         t0 = time.time()
    #         while True:
    #             if stop_event.is_set():
    #                 raise concurrent.futures.CancelledError()
    #             if self.lta_timeout and time.time() - t0 >= self.lta_timeout:
    #                 raise LTAError(
    #                     f"LTA retrieval for {uuid} timed out (lta_timeout={self.lta_timeout} seconds)"
    #                 )
    #             try:
    #                 if self.api.is_online(uuid):
    #                     break
    #                 if statuses[uuid] == DownloadStatus.OFFLINE:
    #                     # Trigger
    #                     triggered = self.trigger_offline_retrieval(uuid)
    #                     if triggered:
    #                         statuses[uuid] = DownloadStatus.TRIGGERED
    #                         self.logger.info(
    #                             "%s accepted for retrieval, waiting for it to come online...", uuid
    #                         )
    #                     else:
    #                         # Product is online
    #                         break
    #             except (LTAError, ServerError) as e:
    #                 self.logger.info(
    #                     "%s retrieval was not accepted: %s. Retrying in %d seconds",
    #                     uuid,
    #                     e.msg,
    #                     self.lta_retry_delay,
    #                 )
    #             _wait(stop_event, self.lta_retry_delay)
    #         self.logger.info("%s retrieval from LTA completed", uuid)
    #         statuses[uuid] = DownloadStatus.ONLINE
    
    def _download_online_retry_new(self, pid: str,name: str, directory,stop_event: threading.Event,statuses):
        print(f"HEREEEEEEEEEEEEEEEEEEEEE {pid}")
        self.download(pid,directory,stop_event)
        print("caso")

    def _download_online_retry(self, pid: str,name: str, directory,stop_event: threading.Event,statuses):
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
            if stop_event.is_set():
                raise concurrent.futures.CancelledError()
            try:
                if cnt > 0:
                    _wait(stop_event, self.dl_retry_delay)
                statuses[uuid] = DownloadStatus.DOWNLOAD_STARTED
                return self.download(uuid, directory, stop_event=stop_event)
            except (concurrent.futures.CancelledError, KeyboardInterrupt, SystemExit):
                raise
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
                else:
                    self.logger.info("Downloading %s failed. No retries left.", title)
                last_exception = e
        raise last_exception

    def _download(self, url, path, file_size, title, stop_event):
        headers = {}
        continuing = path.exists()
        access_token = self.api.token
        if continuing:
            already_downloaded_bytes = 0#TODO remove this 0 #path.stat().st_size
            # input(f"ACCESS TOKEN: {access_token}")
            headers = {"Authorization": f"Bearer {access_token}","Range": "bytes={}-".format(already_downloaded_bytes)}
        else:
            already_downloaded_bytes = 0
            headers = {"Authorization": f"Bearer {access_token}"}
        downloaded_bytes = 0
        # self.api.session.get(url, stream=True, headers=headers)
        with self.api.dl_limit_semaphore:
            print("GETTING DOWNLOAD",self.api.dl_limit_semaphore._value)
            import requests
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
                iterator = r.iter_content(chunk_size=self.chunk_size)
                while True:
                    if stop_event and stop_event.is_set():
                        raise concurrent.futures.CancelledError()
                    try:
                        with self.api.dl_limit_semaphore:
                            chunk = next(iterator)
                    except StopIteration:
                        break
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        progress.update(len(chunk))
                        downloaded_bytes += len(chunk)
            # Return the number of bytes downloaded
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
