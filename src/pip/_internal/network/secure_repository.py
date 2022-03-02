"""
TUF (TheUpdateFramework) integration: Verify that everything served by the
repository (e.g. PyPI) is cryptographically signed.
"""

import hashlib
import logging
import os
import shutil
import urllib.parse
from typing import Dict, List, Optional, Tuple

from pip._vendor.requests import Session
from pip._vendor.tuf.ngclient.updater import Updater, UpdaterConfig
from pip._vendor.tuf.ngclient.fetcher import FetcherInterface
from pip._vendor.tuf.api.exceptions import (
    RepositoryError,
    UnsignedMetadataError,
    DownloadError,
)

from pip._internal.exceptions import ConfigurationError, NetworkConnectionError
from pip._internal.models.link import Link
from pip._internal.network.secure_fetcher import PipFetcher
from pip._internal.utils.temp_dir import TempDirectory

logger = logging.getLogger(__name__)


class SecureRepository:
    """Represents a single package index/repository that we have local
    metadata for. Provides methods to securely download distribution
    and index files from the remote repository."""

    def __init__(
        self, index_url: str, fetcher: FetcherInterface, tuf_metadata_dir: str
    )-> None:

        # Construct unique directory name based on the url
        dir_name = hashlib.sha224(index_url.encode('utf-8')).hexdigest()
        split_url = urllib.parse.urlsplit(index_url)
        self._base_url = urllib.parse.urlunsplit(
            [split_url.scheme, split_url.netloc, '', '', '']
        )

        # targets_path contains index files (for PyPI: "simple/")
        targets_path = split_url.path.lstrip('/')

        # local metadata dir
        metadata_dir = os.path.join(tuf_metadata_dir, dir_name, "metadata")
        metadata_base_url = f"{self._base_url}:9001/metadata/"
        # Store two separate mirror configs. First one is used when downloading index
        # files: in this case both metadata_path and target_path are set.
        # TODO: metadata_path resolution is still open:
        # https://github. com/jku/pip/issues/5
        self._index_mirrors = {
            self._base_url: {
                'url_prefix': self._base_url,
                'metadata_path': 'simple/',
                'targets_path': targets_path,
            }
        }
        # 2nd mirror configuration is used when downloading distributions:
        # in this case only metadata_path is set. Before downloading an additional
        # distribution mirror will be added to this configuration
        self._distribution_mirrors = {
            self._base_url: {
                'url_prefix': self._base_url,
                'metadata_path': 'simple/',
            }
        }

        # TODO how should this TempDir be handled?
        self._tmp_dir = TempDirectory(globally_managed=True).path

        # TODO not clear how to handle distribution mirrors here
        self._updater = Updater(
            metadata_dir=metadata_dir,
            metadata_base_url=metadata_base_url,
        )
        self._refreshed = False

    def download_index(self, project_name):
        # type: (str) -> Optional[bytes]
        """Securely download project index file. Return content of the
        file or None if download did not succeed."""
        try:
            # No progress notification for metadata or index downloads
            self._set_progress_bar("off")

            self._ensure_fresh_metadata()

            target_name = f"{project_name}/{project_name}.html"
            # Fetch the target metadata. If needed, fetch target as well
            target = self._updater.get_targetinfo(target_name)
            if target is None:
                logger.debug(f"Failed to download index for {project_name}")
                return None

            self._updater.config = UpdaterConfig(prefix_targets_with_hash=True)
            path = self._updater.download_target(
                target,
                filepath=os.path.join(self._tmp_dir, project_name),
                target_base_url=f"{self._base_url}/simple/"
            )

            with open(path, "rb") as f:
                return f.read()

        except RepositoryError as e:
            raise RecursionError(e)

        except DownloadError as e:
            logger.warning("Failed to download index for %s: %s", project_name, e)
            return None

    def download_distribution(self, link, location, progress_bar):
        # type: (Link, str, str) -> str
        """Securely download distribution file into 'location'.
        Return path to downloaded file (note that path may include
        new subdirectories under 'location')."""

        # Raises NetworkConnectionError, ?
        # TODO do we need to double check that comes_from matches our index_url?
        try:
            # No progress notification for metadata downloads
            self._set_progress_bar("off")

            self._ensure_fresh_metadata()

            base_url, target_name = self._split_distribution_url(link)
            self._ensure_distribution_mirror_config(base_url)

            # fetch target metadata. If needed, fetch target
            logger.debug("Fetching metadata for %s", target_name)
            self._updater.config = UpdaterConfig(prefix_targets_with_hash=False)
            target = self._updater.get_targetinfo(target_name)
            if target is None:
                logger.debug(f"Failed to download index for {target_name}")
                return None
            path = self._updater.download_target(
                target,
                filepath=os.path.join(location, target_name.split("/")[-1]),
                target_base_url=f"{self._base_url}/packages/"
            )

            #target = self._updater.get_one_valid_targetinfo(target_name)

            # if self._updater.updated_targets([target], location):
            #     #self._set_progress_bar(progress_bar)
            #     logger.info("Downloading %s", target_name)
            #     self._updater.download_target(
            #         target, location, prefix_filename_with_hash=False
            #     )
            # else:
            #     logger.info("Already downloaded %s", target)

            return path

        except UnsignedMetadataError as e:
            raise UnsignedMetadataError(e)

        except RepositoryError as e:
            raise RepositoryError(e)

    def _set_progress_bar(self, progress_bar):
        # type: (str) -> None
        self._updater._fetcher.progress_bar = progress_bar

    def _ensure_fresh_metadata(self):
        # type: () -> None
        """Ensure metadata is refreshed exactly once"""

        if not self._refreshed:
            # TODO Raises ?
            self._updater.refresh()
            self._refreshed = True

    def _ensure_distribution_mirror_config(self, mirror_url):
        # type: (str) -> None
        """Ensure the given url is included in the distribution mirror configuration"""

        if mirror_url not in self._distribution_mirrors:
            # A distribution mirror only serves targets (distribution files):
            # do not set metadata_path.
            self._distribution_mirrors[mirror_url] = {
                'url_prefix': mirror_url,
                'targets_path': '',
            }

    def _split_distribution_url(self, link):
        # type: (Link) -> Tuple[str, str]
        """Split link url into base path and target name"""

        # "https://files.pythonhosted.org/packages/8f/1f/74aa91b56dea5847b62e11ce6737db82c6446561bddc20ca80fa5df025cc/Django-1.1.3.tar.gz#sha256=..."
        #    ->
        # ("https://files.pythonhosted.org/packages/",
        #  "8f/1f/74aa91b56dea5847b62e11ce6737db82c6446561bddc20ca80fa5df025cc/Django-1.1.3.tar.gz")

        split_path = link.path.split('/')

        # NOTE: knowledge of path structure is required to do the split here
        # target name is filename plus three directory levels to form full blake hash.
        # Sanity check: does path contain directory names that form blake2b hash
        blake2b = ''.join(split_path[-4:-1])
        if len(blake2b) != 64:
            raise ValueError('Expected structure not found in link "{}"'.format(link))

        target_name = '/'.join(split_path[-4:])
        base_path = '/'.join(split_path[:-4])
        base_url = urllib.parse.urlunsplit(
            [link.scheme, link.netloc, base_path, '', '']
        )
        return base_url, target_name


class SecureRepositoryManager:
    """"Manager for all the SecureRepository objects currently in use"""

    # URLS from these indexes should always end up being downloaded with
    # SecureRepository: it should be an error to do otherwise
    # TODO: this should contain "https://pypi.org/simple/" once pypi supports TUF
    _KNOWN_SECURE_INDEXES = [
        # "https://pypi.org/simple/",
        "http://localhost:9001/simple/",
    ]

    def __init__(self, index_urls, data_dir, session):
        # type: (Optional[List[str]], Optional[str], Session) -> None

        logger.debug("Initializing SecureRepositoryManager")

        # Use temporary directory if datadir is not available
        if data_dir is None:
            data_dir = TempDirectory(globally_managed=True).path
        tuf_metadata_dir = os.path.join(data_dir, 'tuf')

        # Bootstrap metadata with installed metadata (if not done already)
        self._bootstrap_metadata(tuf_metadata_dir)

        # tuf_settings.repositories_directory = tuf_metadata_dir

        self._repositories = self._initialize_repositories(
            index_urls,
            session,
            tuf_metadata_dir,
        )

    def get_secure_repository(self, project_url):
        # type: (str) -> Tuple[Optional[SecureRepository], str]
        """Return SecureRepository for given index url, or None.
           Also return the name of the project"""

        index_url, _, project = str(project_url).rstrip('/').rpartition('/')
        if not project:
            raise ValueError(
                'Failed to parse {} as project index URL'.format(project_url)
            )

        index_url = self._canonicalize_url(index_url)
        repository = self._repositories.get(index_url)

        # security double check: make sure PyPI is a match
        if repository is None and index_url in self._KNOWN_SECURE_INDEXES:
            raise ConfigurationError(
                'Expected to find secure downloader for {}'.format(index_url)
            )

        return (repository, project)

    # Bootstrap the TUF metadata with metadata shipped with pip
    # (only if that TUF metadata does not exist yet).
    # Raises OSErrors like FileExistsError
    # TODO: handle failures better: e.g. if bootstrap fails somehow, maybe remove the directory
    def _bootstrap_metadata(self, metadata_dir: str) -> None:
        bootstrapdir = os.path.join(
            os.path.dirname(__file__),
            "secure_repository_bootstrap"
        )

        for bootstrap in os.listdir(bootstrapdir):
            # check if metadata matching this name already exists
            dirname = os.path.join(metadata_dir, bootstrap)
            if os.path.exists(dirname):
                continue

            # create the structure TUF expects
            logger.debug("Bootstrapping TUF metadata for '%s'", bootstrap)
            os.makedirs(os.path.join(dirname, "metadata"))
            shutil.copyfile(
                os.path.join(bootstrapdir, bootstrap, "root.json"),
                os.path.join(os.path.join(dirname, "metadata"), "root.json")
            )

    @staticmethod
    def _initialize_repositories(
        index_urls: Optional[List[str]], session: Session, data_dir: str
    )-> Dict[str, SecureRepository]:

        """Return a Dictionary of Repositories: one repository per index url
        but only if we found local metadata for that index url. """

        repositories = {}
        fetcher = PipFetcher(session)

        for index_url in index_urls or []:
            index_url = SecureRepositoryManager._canonicalize_url(index_url)
            try:
                repository = SecureRepository(index_url, fetcher, data_dir)
                repositories[index_url] = repository
                logger.debug('Secure repository initialized for %s', index_url)
            except MissingLocalRepositoryError:
                logger.debug('No secure repository metadata for %s', index_url)
                if index_url in SecureRepositoryManager._KNOWN_SECURE_INDEXES:
                    raise ConfigurationError(
                        'Expected to find secure repository metadata for {}'.format(index_url)
                    )
            except RepositoryError:
                # Something is wrong with the local metadata
                # TODO review tuf to see what we should do here
                raise ConfigurationError(
                    'Failed to load secure repository configuration'
                )

        return repositories

    @staticmethod
    def _canonicalize_url(index_url):
        # type: (str) -> str

        # TODO: Should we canonicalize anything else?
        # This is most relevant for making sure that we find the repo metadata directory
        # using the index url given on the command line or pip.conf
        if index_url[-1] != '/':
            index_url = index_url + '/'
        return index_url
