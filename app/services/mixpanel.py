import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json
load_dotenv()

class MixPanelServices:
    URL = "https://data.mixpanel.com/api/2.0/export"
    MIXPANEL_API_KEY = os.getenv("MIXPANEL_API_SECRET_RIPPLE")
    def __init__(self):
        if not self.MIXPANEL_API_KEY:
            raise ValueError("MIXPANEL_API_KEY is not set")
    
    def fetch_events(self, from_date: str, to_date: str):
        """
        Fetches events from Mixpanel API.
        """
        params = {
            "from_date": from_date,
            "to_date": to_date,
        }
        response = requests.get(self.URL, params=params, auth=(self.MIXPANEL_API_KEY, ""), timeout=60)
        response.raise_for_status()
        events = []
        for line in response.text.strip().split("\n"):
            events.append(json.loads(line))
        return events
    def fetch_events_for_yesterday(self):
        yesterday = datetime.utcnow() - timedelta(days=1)
        date_str = yesterday.strftime("%Y-%m-%d")
        return self.fetch_events(from_date=date_str, to_date=date_str)

mixPanelServices = MixPanelServices()