"""ServiceTitan Authentication."""

from __future__ import annotations

from typing import Any

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class ServiceTitanAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for ServiceTitan."""

    @property
    def oauth_request_body(self) -> dict[str, Any]:
        """Define the OAuth request body for the AutomaticTestTap API.

        Returns:
            A dict with the request body
        """
        return {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
