import logging
from typing import Iterable

from pip._vendor.requests import Session
from pip._vendor.requests.exceptions import HTTPError
from pip._vendor.requests.models import CONTENT_CHUNK_SIZE, Response
from pip._vendor.tuf.ngclient.fetcher import FetcherInterface
from pip._vendor.tuf.api.exceptions import DownloadHTTPError

from pip._internal.cli.progress_bars import get_download_progress_renderer
from pip._internal.network.utils import response_chunks, should_show_progress

logger = logging.getLogger(__name__)


class PipFetcher(FetcherInterface):
    """Implementation of TUF FetcherInterface, used by SecureRepository. It
    allows Pip to a) control download details by reusing the same
    response_chunks() mechanisms as and b) handle progress notifications"""

    def __init__(self, session):
        # type: (Session) -> None
        self._session = session
        # default value: caller should change this as needed
        self.progress_bar = "on"

    def fetch(self, url):
        # type: (str, int) -> Iterable[bytes]

        # TODO set headers, maybe reuse download._http_get_download()?
        response = self._session.get(url, stream=True)  # type: Response
        try:
            response.raise_for_status()
        except HTTPError as e:
            status = e.response.status_code
            raise DownloadHTTPError(str(e), status)

        # TODO reuse code in download._prepare_download()?
        chunks = response_chunks(response, CONTENT_CHUNK_SIZE)
        if not should_show_progress(response, logger.getEffectiveLevel()):
            return chunks

        return get_download_progress_renderer(
            self.progress_bar
        )(chunks)
