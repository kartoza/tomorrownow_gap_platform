# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Monkey patch for Salient function.
"""


import os
import logging
import requests
import pandas as pd


logger = logging.getLogger(__name__)
CHUNK_DOWNLOAD_SIZE = 250 * 1024 * 1024


def patch_download_query(
    query: str,
    file_name: str,
    format: str = "-auto",
    force: bool = False,
    session: requests.Session | None = None,
    verify: bool = None,
    verbose: bool = False,
    strict: bool = True,
) -> str:
    """
    Download the query result and save it to a file.

    This function handles the downloading of data based on
    the provided query URL. It saves the data to the specified
    file name.
    :param query: The URL from which to download the data.
    :type query: str
    :param file_name: The path where the data will be saved.
    :type file_name: str
    :param format: The format of the file., defaults to "-auto"
    :type format: str, optional
    :param force: If False (default) skips downloading `file_name`
        if it already exists., defaults to False.
    :type force: bool, optional
    :param session: The session to use for the download.
        If `None` (default) uses `get_current_session()`.
    :type session: requests.Session | None, optional
    :param verify: Whether to verify the server's TLS certificate.
        If `None` (default) uses the current verification setting
        via `get_verify_ssl()`.
    :type verify: bool, optional
    :param verbose: If True, prints additional output about
        the download process., defaults to False
    :type verbose: bool, optional
    :param strict: If True (default) raises errors if
        there is a problem., defaults to True
    :type strict: bool, optional
    :raises requests.HTTPError: If the server returns an error status code.
    :return: The file name of the downloaded data. When
        strict=False, will return pd.NA if there was an error.
    :rtype: str
    """
    from salientsdk.login_api import get_current_session, get_verify_ssl
    if format == "-auto":
        # extract the file extension from the file name
        format = file_name.split(".")[-1]

    if session is None:
        session = get_current_session()

    verify = get_verify_ssl(verify)

    result = None
    if force or not os.path.exists(file_name):
        if verbose:
            print(
                f"Downloading\n  {query}\n to "
                f"{file_name}\n with {session} "
                f" with chunk_size = {CHUNK_DOWNLOAD_SIZE}")
        with session.get(query, verify=verify, stream=True) as result:
            try:
                result.raise_for_status()
            except Exception as e:
                if strict:
                    raise e
                else:
                    logger.error(f"Error downloading {query}:")
                    logger.error(e)
                    return pd.NA
            with open(file_name, "wb" if format == "nc" else "w") as f:
                # use chunk download the files
                for chunk in result.iter_content(
                    chunk_size=CHUNK_DOWNLOAD_SIZE
                ):
                    if chunk:  # Filter out keep-alive new chunks
                        if format == "nc":
                            f.write(chunk)
                        else:
                            f.write(chunk.decode('utf-8'))
    elif verbose:
        print(f"File {file_name} already exists")

    return file_name
