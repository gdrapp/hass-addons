import requests
import requests.adapters
import urllib3


class SupervisorAPIError(Exception):
    pass


class _BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


class SupervisorAPI:
    BASE_URL = "http://supervisor"

    def __init__(self, token: str):
        """Interact with the Home Assistant Supervisor API

        Args:
            token (str): Supervisor bearer token
        """
        self.auth = _BearerAuth(token)
        self.session = requests.Session()
        retry_strategy = urllib3.Retry(
            total=10, backoff_factor=10, status_forcelist=[400, 500, 502, 503, 504]
        )
        self.session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retry_strategy))

    def _get(self, path: str) -> requests.Response:
        url = f"{SupervisorAPI.BASE_URL}{path}"
        try:
            response = self.session.get(url, auth=self.auth)
        except (requests.exceptions.ConnectionError, requests.exceptions.RetryError) as err:
            raise SupervisorAPIError(
                f"Error connecting to Home Assistant Supervisor API: {err}")
        except requests.exceptions.Timeout as err:
            raise SupervisorAPIError(
                "Timeout connecting to Home Assistant Supervisor API")
        else:
            json = None
            if response.ok:
                try:
                    json = response.json()
                except ValueError as err:
                    raise SupervisorAPIError(
                        "Error decoding response from Home Assistant Supervisor API")
            return json

    def _post(self, path: str) -> requests.Response:
        url = f"{SupervisorAPI.BASE_URL}{path}"
        try:
            response = self.session.post(url, auth=self.auth)
        except (requests.exceptions.ConnectionError, requests.exceptions.RetryError) as err:
            raise SupervisorAPIError(
                f"Error connecting to Home Assistant Supervisor API: {err}")
        except requests.exceptions.Timeout as err:
            raise SupervisorAPIError(
                "Timeout connecting to Home Assistant Supervisor API")
        else:
            json = None
            if response.ok:
                try:
                    json = response.json()
                except ValueError as err:
                    raise SupervisorAPIError(
                        "Error decoding response from Home Assistant Supervisor API")
            return json

    def get_snapshots(self):
        """Get list of all snapshots

        Returns:
            List: List of snapshots
        """
        response = self._get("/snapshots")
        return response.get("data", {}).get("snapshots", [])

    def get_snapshot(self, slug: str):
        """Get details of a single snapshot

        Args:
            slug (str): Slug of snapshot to retrieve

        Returns:
            dict: Dictionary containing snapshot details
        """
        response = self._get(f"/snapshots/{slug}/info")
        return response.get("data")

    def remove_snapshot(self, slug: str) -> bool:
        """Delete a snapshot

        Args:
            slug (str): Slug of snapshot to delete

        Returns:
            bool: True if successful
        """
        response = self._post(f"/snapshots/{slug}/remove")
        return True if response.get("result") == "ok" else False
