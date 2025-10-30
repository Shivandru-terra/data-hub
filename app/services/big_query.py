import io
import json
from datetime import datetime, timezone
from google.cloud import bigquery
from app.services.mixpanel import mixPanelServices
import math
# import re
class BigQueryServices:
    TOP_LEVEL_FIELDS = ["dataset", "time", "distinct_id", "name"]
    PROPERTIES_FIELDS = [
        "mp_processing_time_ms",
        "mp_lib",
        "username",
        "device_os",
        "app_version_on_install",
        "time",
        "screen_height",
        "mp_api_timestamp_ms",
        "os_version",
        "city",
        "screen_dpi",
        "mp_api_endpoint",
        "model",
        "has_nfc",
        "insert_id",
        "lib_version",
        "is_internal",
        "os",
        "had_persisted_distinct_id",
        "region",
        "has_telephone",
        "screen_width",
        "brand",
        "device_id",
        "mp_country_code",
        "bluetooth_version",
        "bluetooth_enabled",
        "carrier",
        "wifi",
        "user_id",
        "app_build_number",
        "manufacturer",
        "app_version_string",
        "app_version",
        "app_release",
        "gameName",
        "radio",
        "age",
        "d0_games",
        "days_since_install",
        "display_name",
        "sessionId",
        "session_token",
        "app_game_count",
        "game_entry_count",
        "game_session_id",
        "timeSpent",
        "game_values",
    ]
    GAME_VALUES_SCHEMA = {
        "aicrazygirlfriend": [
            "Conversation_Delta_Alertness",
            "Conversation_Delta_Trust",
            "FTUE_Dpad_Moved",
            "FTUE_Escape",
            "FTUE_Got_away_time",
            "FTUE_Interaction",
            "FTUE_Intro_End",
            "FTUE_Kill",
            "FTUE_Pan",
            "FTUE_Scene_Loaded",
            "FTUE_yuki_spoke",
            "GameID",
            "Player_Killed",
            "Player_escaped",
            "Puzzle_Interacted",
            "Sus_Object_Seen",
            "Time_Away_trigger",
            "Time_Spent_Escaped",
            "Time_Spent_Killed",
            "Total_no_Chats",
        ],
        "mafia": [
            "FTUE_Conversation_end",
            "FTUE_Conversation_started",
            "FTUE_DPAD_used",
            "FTUE_First_Knock",
            "FTUE_PAN_used",
            "FTUE_costume_switch_Wardrobe",
            "FTUE_time_taken",
        ],
        "vampireai": [
            "FTUE_Conversation_end",
            "FTUE_Conversation_started",
            "FTUE_DPAD_used",
            "FTUE_First_Knock",
            "FTUE_PAN_used",
            "FTUE_Scene_loaded",
            "FTUE_costume_switch_Wardrobe",
            "FTUE_time_taken",
            "avg_time_per_conversation",
            "bat_transformations",
            "character_disguise",
            "chat_time",
            "ending_reached",
            "game_id",
            "houses_knocked",
            "houses_lost",
            "houses_won",
            "invited",
            "no_of_chats",
            "no_times_disguises_changed",
            "npc_pattern",
            "police_escape",
            "police_level",
            "police_starting_trust",
            "police_state_triggered",
            "time_spent_in_each_disguise",
            "total_police_interactions",
            "total_time_spent_police",
        ],
    }

    def __init__(self, client: bigquery.Client):
        self.client = client
    def to_utc_iso(self, raw):
        """
        Convert different raw timestamp formats to an ISO8601 UTC timestamp string.
        Accepts:
        - int or float (seconds / milliseconds / microseconds)
        - numeric string ("1623456789000")
        - ISO timestamp string ("2025-10-21T07:31:27.000Z")
        - datetime.datetime (aware or naive)
        Returns:
        - ISO 8601 string with timezone offset (e.g. '2025-10-23T05:34:12+00:00')
        - None if input is None or cannot be parsed
        This function tries to *auto-detect* unit by magnitude and avoids double-conversion.
        """
        if raw is None:
            return None

        # If already a datetime
        if isinstance(raw, datetime):
            dt = raw
            if dt.tzinfo is None:
                # assume UTC if naive
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()

        # If it's a string, try ISO parse first
        if isinstance(raw, str):
            raw_str = raw.strip()
            # Common ISO formats detection
            try:
                # datetime.fromisoformat accepts many formats (py3.11+ with offsets)
                # Try standard parse: accept trailing Z
                if raw_str.endswith("Z"):
                    # replace Z with +00:00 for fromisoformat
                    iso = raw_str.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(iso)
                    return dt.astimezone(timezone.utc).isoformat()
                else:
                    dt = datetime.fromisoformat(raw_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc).isoformat()
            except Exception:
                # not ISO: maybe numeric string
                try:
                    # fall through to numeric parse
                    if raw_str.isdigit():
                        num = float(raw_str)
                    else:
                        num = float(raw_str)
                except Exception:
                    return None
        else:
            # not str, not datetime: try numeric
            try:
                num = float(raw)
            except Exception:
                return None

        # Now we have numeric `num`. Determine unit by magnitude.
        # Typical ranges (approx):
        #   seconds since epoch: ~1e9  (2001) -> ~1.7e9 (2024)
        #   milliseconds: ~1e12
        #   microseconds: ~1e15 or higher
        # If num is extremely large (>1e18) it's invalid / probably garbage.
        if math.isfinite(num):
            absnum = abs(num)
            # If it looks like microseconds (>= 1e14)
            if absnum >= 1e14:
                # microseconds -> seconds
                ts_seconds = num / 1e6
            elif absnum >= 1e11:
                # milliseconds -> seconds
                ts_seconds = num / 1e3
            elif absnum >= 1e9:
                # seconds -> seconds
                ts_seconds = num
            else:
                # Too small to be epoch seconds (likely wrong)
                # Return None to avoid inserting epoch date
                return None
        else:
            return None

        # Validate reasonable timestamp range (allowable for BigQuery)
        # BigQuery TIMESTAMP range roughly corresponds to years 0001-9999.
        # Here we check seconds between year 1970 and some far future (e.g., year 10000).
        if ts_seconds < 0 or ts_seconds > 253402300799:  # seconds for 9999-12-31
            return None

        try:
            dt = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
            return dt.isoformat()
        except Exception:
            return None
    def build_bq_row(self, event: dict) -> dict:
        cleaned_row = {}
        properties = {}
        game_values = {}

        # Get the properties object from the event
        props = event.get("properties", {})

        # Clean and map fields
        for key, value in props.items():
            # remove '$' prefix if present
            clean_key = key.lstrip('$')
            val = value if value != "" else None

            if clean_key == "time":
                converted = self.to_utc_iso(val)
                val = converted

            if clean_key in self.PROPERTIES_FIELDS:
                properties[clean_key] = val
            elif clean_key in self.TOP_LEVEL_FIELDS:
                cleaned_row[clean_key] = val

        # Handle game_values
        game_name = properties.get("gameName")
        if game_name and game_name in self.GAME_VALUES_SCHEMA:
            for gv_key in self.GAME_VALUES_SCHEMA[game_name]:
                if gv_key in props and props[gv_key] != "":
                    game_values[gv_key] = props[gv_key]

        properties["game_values"] = json.dumps(game_values) if game_values else "{}"
        cleaned_row["properties"] = properties
        return cleaned_row

    def upload_events(self, events: list, table_id: str, batch_size: int = 500):
        """
        Upload events directly from Mixpanel API response (list of dicts)
        """
        rows_to_insert = []
        for i, event in enumerate(events, start=1):
            bq_row = self.build_bq_row(event)
            raw_time = event.get("properties", {}).get("time")
            bq_row["time"] = self.to_utc_iso(raw_time)
            # Add top-level fields like 'dataset', 'time', 'distinct_id', 'name'
            bq_row["dataset"] = "mixpanel"
            bq_row["distinct_id"] = event["properties"].get("distinct_id")
            bq_row["name"] = event.get("event")
            insert_id = event.get("properties", {}).get("$insert_id")

            if insert_id:
                bq_row["properties"]["insert_id"] = insert_id

            rows_to_insert.append(bq_row)
            if len(rows_to_insert) >= batch_size:
                errors = self.client.insert_rows_json(table_id, rows_to_insert)
                if errors:
                    print(f"❌ Error inserting rows at batch ending {i}: {errors}")
                else:
                    print(f"✅ Uploaded {i} rows...")
                rows_to_insert = []

        if rows_to_insert:
            errors = self.client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                print(f"❌ Error inserting final rows: {errors}")
            else:
                print(f"✅ Uploaded {i} rows total.")
        print("🎯 Upload complete!")


bigquery_client = bigquery.Client()

bigQueryServices = BigQueryServices(bigquery_client)

# events = mixPanelServices.fetch_events("2025-10-17", "2025-10-28")
# bigQueryServices.upload_events(events, "ai-analytics-463910.Ripple.Event")
