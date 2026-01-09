import requests
import uuid
import logging
from typing import Optional


class PowerBIClient:
    def __init__(self, cluster_url: str, report_id: str, embed_token: str):
        self.cluster_url = cluster_url
        self.report_id = report_id
        self.logger = logging.getLogger(self.__class__.__name__)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"EmbedToken {embed_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def get_models_and_exploration(self) -> dict:
        url = (
            f"{self.cluster_url}/explore/reports/{self.report_id}/modelsAndExploration"
        )
        params = {"preferReadOnlySession": "true", "skipQueryData": "true"}

        headers = {
            "activityid": str(uuid.uuid4()),
            "requestid": str(uuid.uuid4()),
            "origin": "https://app.powerbi.com",
            "referer": "https://app.powerbi.com/",
        }

        self.logger.info("Fetching modelsAndExploration")
        response = self.session.get(url, params=params, headers=headers, timeout=60)
        response.raise_for_status()

        return response.json()

    def execute_query(self, qes_endpoint: str, mwc_token: str, payload: dict) -> dict:
        headers = {
            "Authorization": f"MWCToken {mwc_token}",
            "Content-Type": "application/json;charset=UTF-8",
            "Accept": "application/json, text/plain, */*",
            "activityid": str(uuid.uuid4()),
            "requestid": str(uuid.uuid4()),
            "x-ms-root-activity-id": str(uuid.uuid4()),
            "x-ms-parent-activity-id": str(uuid.uuid4()),
            "origin": "https://app.powerbi.com",
            "referer": "https://app.powerbi.com/",
        }

        self.logger.info("Executing semantic query")
        response = self.session.post(
            qes_endpoint, json=payload, headers=headers, timeout=120
        )

        if response.status_code != 200:
            self.logger.error("QES error %s: %s", response.status_code, response.text)

        response.raise_for_status()
        return response.json()

    def get_mwc_token(self, model_exploration_data: dict) -> Optional[str]:
        def find_key(obj, key):
            if isinstance(obj, dict):
                if key in obj:
                    return obj[key]
                for v in obj.values():
                    out = find_key(v, key)
                    if out is not None:
                        return out
            elif isinstance(obj, list):
                for v in obj:
                    out = find_key(v, key)
                    if out is not None:
                        return out
            return None

        mwc = find_key(model_exploration_data, "mwcToken")

        mwc_found = bool(mwc)
        self.logger.info(f"MWC searching status: {mwc_found}")
        if mwc_found:
            self.logger.info(f"MWC token length: {len(mwc)}")

        return mwc
