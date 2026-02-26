from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class SecretManagerStore:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self._client = None

    def _svc(self):
        if self._client is not None:
            return self._client
        from google.cloud import secretmanager

        self._client = secretmanager.SecretManagerServiceClient()
        return self._client

    def _secret_path(self, name: str) -> str:
        return f"projects/{self.project_id}/secrets/{name}"

    def _version_path(self, name: str, version: str = "latest") -> str:
        return f"{self._secret_path(name)}/versions/{version}"

    def get_secret(self, name: str, version: str = "latest", default: str | None = None) -> str | None:
        try:
            resp = self._svc().access_secret_version(request={"name": self._version_path(name, version)})
            return resp.payload.data.decode("utf-8")
        except Exception:
            logger.debug("Secret read failed for %s", name, exc_info=True)
            return default

    def add_secret_version(self, name: str, value: str) -> None:
        self._svc().add_secret_version(
            request={"parent": self._secret_path(name), "payload": {"data": value.encode("utf-8")}}
        )

