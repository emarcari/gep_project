import requests
import logging


class EmbedTokenError(RuntimeError):
    """Raised when the EmbedToken service fails or returns an invalid response."""

    pass


class EmbedTokenProvider:
    def __init__(self, token_url: str, timeout: int = 60):
        self.token_url = token_url
        self.timeout = timeout
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_token(self) -> str:
        """
        Retrieve an EmbedToken from the internal token service.

        :param timeout: HTTP timeout in seconds
        :return: EmbedToken string
        :raises EmbedTokenError: if the token cannot be retrieved
        """
        self.logger.info("Requesting EmbedToken")

        try:
            response = requests.get(self.token_url, timeout=self.timeout)
            response.raise_for_status()

        except requests.RequestException as exc:
            self.logger.error("HTTP error while requesting EmbedToken", exc_info=exc)
            raise EmbedTokenError("Failed to call token service") from exc

        try:
            data = response.json()
        except ValueError as exc:
            self.logger.error("Token service response is not valid JSON")
            raise EmbedTokenError("Invalid JSON response from token service") from exc

        token = data.get("Token")
        if not token:
            self.logger.error("Field 'Token' not found in response: %s", data)
            raise EmbedTokenError("EmbedToken not returned by token service")

        self.logger.info("EmbedToken retrieved successfully")
        return token
