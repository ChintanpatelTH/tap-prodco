"""ProdcoSource Authentication."""

from __future__ import annotations

import requests
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from singer_sdk.helpers._util import utc_now


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class ProdcoSourceAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for ProdcoSource."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the AutomaticTestTap API.

        Returns:
            A dict with the request body
        """
        # TODO: Define the request body needed for the API.
        return {
            "resource": "https://api.prodcotech.com/token",
            "username": self.config["username"],
            "password": self.config["password"],
            "grant_type": "password",
        }

    @classmethod
    def create_for_stream(cls, stream) -> ProdcoSourceAuthenticator:  # noqa: ANN001
        """Instantiate an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator.
        """
        return cls(
            stream=stream,
            auth_endpoint="https://api.prodcotech.com/token",
        )
