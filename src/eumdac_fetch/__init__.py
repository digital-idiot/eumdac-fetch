"""eumdac-fetch: Bulk EUMETSAT data downloader."""

__version__ = "1.0.0"

from eumdac import AccessToken

from eumdac_fetch.auth import create_token, get_token
from eumdac_fetch.dataset import RemoteDataset
from eumdac_fetch.env import ENV
from eumdac_fetch.remote import TokenRefreshingHTTPFileSystem

__all__ = [
    "AccessToken",
    "ENV",
    "TokenRefreshingHTTPFileSystem",
    "RemoteDataset",
    "create_token",
    "get_token",
    "__version__",
]
