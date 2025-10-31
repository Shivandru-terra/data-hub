import io
import json
from datetime import datetime, timezone
from google.cloud import bigquery
from app.services.mixpanel import mixPanelServices
import math
# import re
class BigQueryServices:
    TOP_LEVEL_FIELDS_RIPPLE = ["dataset", "time", "distinct_id", "name"]
    PROPERTIES_FIELDS_RIPPLE = [
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
    GAME_VALUES_SCHEMA_RIPPLE = {
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

            if clean_key in self.PROPERTIES_FIELDS_RIPPLE:
                properties[clean_key] = val
            elif clean_key in self.TOP_LEVEL_FIELDS_RIPPLE:
                cleaned_row[clean_key] = val

        # Handle game_values
        game_name = properties.get("gameName")
        if game_name and game_name in self.GAME_VALUES_SCHEMA_RIPPLE:
            for gv_key in self.GAME_VALUES_SCHEMA_RIPPLE[game_name]:
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

class TerraDataBigQueryServices:
    TOP_LEVEL_FIELDS_TERRA = ["dataset", "time", "distinct_id", "name"]
    
    PROPERTIES_FIELDS_TERRA = [
        "event_name",
        "app_version",
        "city",
        "device",
        "distinct_id_before_identity",
        "insert_id",
        "library_version",
        "model",
        "api_endpoint",
        "api_timestamp",
        "operating_system",
        "os_version",
        "radio",
        "region",
        "screen_dpi",
        "screen_height",
        "screen_width",
        "wifi",
        "ab_on_install",
        "ab_testing",
        "ab_testing_group_user_data",
        "active_days",
        "app_version_on_install",
        "cpu_cores",
        "days_since_install",
        "device_model",
        "device_name",
        "device_type",
        "display_name",
        "first_avatar_gender",
        "gaid",
        "gpu",
        "graphics_api",
        "graphics_quality",
        "group_id",
        "has_gyro",
        "invite_code",
        "is_editor",
        "max_streak",
        "notification_permission_status",
        "processor_type",
        "ram",
        "registration_date",
        "resolution",
        "terra_id",
        "uuid",
        "user_age",
        "user_name",
        "vram",
        "weeks_since_install",
        "af_channel",
        "af_status",
        "app_game_count",
        "config_ge_enabled",
        "config_hs_url",
        "cycle_id",
        "d0_ftue_game",
        "day_of_game",
        "entered_from",
        "event_time_stamp",
        "friends_network",
        "game_entry_count",
        "game_genre",
        "game_icon_size",
        "game_load_time",
        "game_name",
        "game_orientation",
        "game_position",
        "game_session_id",
        "hs_ai_avatar_count",
        "hs_ai_avatar_id",
        "hs_ai_avatar_id_shown",
        "hs_ai_avatar_styles_tried",
        "hs_app_minimised_count",
        "hs_avatar_image_change",
        "hs_avatar_page_opens",
        "hs_bookmarked",
        "hs_comments_hearted_count",
        "hs_default_comments_count",
        "hs_game_invite_popup_seen",
        "hs_gi_comment_sm_clicked",
        "hs_gi_game_names",
        "hs_gi_oc_clicked_count",
        "hs_gi_player_profile",
        "hs_gi_time_spent",
        "hs_h2h_page_player_profile",
        "hs_oc_code_scr_sec",
        "hs_oc_gen_ct",
        "hs_oc_cancelled",
        "hs_oc_details_popup_seen",
        "hs_oc_game_gen_ct_v2",
        "hs_oc_timeout",
        "hs_scroll_used_percent",
        "hs_search_1",
        "hs_time_spent",
        "hs_typed_comments_count",
        "is_debug_menu_enabled",
        "is_internal",
        "is_video",
        "last_filter",
        "media_source",
        "country",
        "mixpanel_library",
        "time_processed_utc",
        "net_connected_count",
        "oc_id",
        "player_level",
        "r_score",
        "scene_load_time",
        "session_id",
        "total_milestones",
        "no_internet_popup_shown",
        "app_minimised_count",
        "end_screen_type",
        "exit_reason",
        "frame_rate",
        "funnel_step",
        "game_ftue_data",
        "game_levels",
        "game_value1",
        "game_value2",
        "game_value3",
        "game_value4",
        "game_value5",
        "game_value6",
        "game_value7",
        "game_value8",
        "game_value9",
        "game_value10",
        "game_value11",
        "game_value12",
        "game_value13",
        "game_value14",
        "game_value15",
        "game_value16",
        "game_value17",
        "game_value18",
        "game_value19",
        "game_value20",
        "in_leaderboard_opened",
        "items_used",
        "leaderboard_place",
        "level_load_times",
        "master_api_request_time",
        "score_earned",
        "session_delta_milestone",
        "share_count",
        "share_ids",
        "shared_feed",
        "shared_wa",
        "sm_popup_seen",
        "tc1_spent",
        "tc1_badge",
        "tc1_earned",
        "terra_api_delayed",
        "time_spent",
        "xp_earned",
        "multiplayer_source",
        "net_code_gen_ct",
        "net_code_succ_ct",
        "net_code_type_ct",
        "net_connected_players",
        "net_lobby_invite_challenge_ct",
        "oc_connected_players",
        "analytics_version",
        "mvp_version",
        "exp_homescreen_tabs",
        "is_maximise",
        "api_data_sent",
        "api_error",
        "api_result",
        "api_server_response",
        "api_url",
        "anonymous_id",
        "identified_id",
        "manufacturer",
        "share_feed_count",
        "deep_link_value",
        "hs_ai_avatar_style",
    ]

    # Field name mappings from CSV headers to BigQuery snake_case
    FIELD_MAPPINGS = {
        "Event Name": "event_name",
        "Time": "time",
        "Distinct ID": "distinct_id",
        "App Version": "app_version",
        "City": "city",
        "Device": "device",
        "Distinct ID Before Identity": "distinct_id_before_identity",
        "Insert ID": "insert_id",
        "Library Version": "library_version",
        "Model": "model",
        "API Endpoint": "api_endpoint",
        "API Timestamp": "api_timestamp",
        "Operating System": "operating_system",
        "OS Version": "os_version",
        "Radio": "radio",
        "Region": "region",
        "Screen DPI": "screen_dpi",
        "Screen Height": "screen_height",
        "Screen Width": "screen_width",
        "Wifi": "wifi",
        "AB_on_install": "ab_on_install",
        "AB_testing": "ab_testing",
        "AB_testing_group_user_data": "ab_testing_group_user_data",
        "ActiveDays": "active_days",
        "AppVersion_on_install": "app_version_on_install",
        "CPUCores": "cpu_cores",
        "DaysSinceInstall": "days_since_install",
        "DeviceModel": "device_model",
        "DeviceName": "device_name",
        "DeviceType": "device_type",
        "DisplayName": "display_name",
        "FirstAvatarGender": "first_avatar_gender",
        "GAID": "gaid",
        "GPU": "gpu",
        "GraphicsAPI": "graphics_api",
        "GraphicsQuality": "graphics_quality",
        "GroupID": "group_id",
        "HasGyro": "has_gyro",
        "InviteCode": "invite_code",
        "IsEditor": "is_editor",
        "MaxStreak": "max_streak",
        "NotificationPermissionStatus": "notification_permission_status",
        "OSVersion": "os_version",
        "OperatingSystem": "operating_system",
        "ProcessorType": "processor_type",
        "RAM": "ram",
        "RegistrationDate": "registration_date",
        "Resolution": "resolution",
        "TerraID": "terra_id",
        "UUID": "uuid",
        "UserAge": "user_age",
        "UserName": "user_name",
        "VRAM": "vram",
        "WeeksSinceInstall": "weeks_since_install",
        "af_channel": "af_channel",
        "af_status": "af_status",
        "app_game_count": "app_game_count",
        "config_ge_enabled": "config_ge_enabled",
        "config_hs_url": "config_hs_url",
        "cycle_id": "cycle_id",
        "d0_ftue_game": "d0_ftue_game",
        "day_of_game": "day_of_game",
        "entered_from": "entered_from",
        "event_time_stamp": "event_time_stamp",
        "friendsNetwork": "friends_network",
        "game_entry_count": "game_entry_count",
        "game_genre": "game_genre",
        "game_icon_size": "game_icon_size",
        "game_load_time": "game_load_time",
        "game_name": "game_name",
        "game_orientation": "game_orientation",
        "game_position": "game_position",
        "game_session_id": "game_session_id",
        "hs_ai_avatar_count": "hs_ai_avatar_count",
        "hs_ai_avatar_id": "hs_ai_avatar_id",
        "hs_ai_avatar_id_shown": "hs_ai_avatar_id_shown",
        "hs_ai_avatar_styles_tried": "hs_ai_avatar_styles_tried",
        "hs_app_minimised_count": "hs_app_minimised_count",
        "hs_avatar_image_change": "hs_avatar_image_change",
        "hs_avatar_page_opens": "hs_avatar_page_opens",
        "hs_bookmarked": "hs_bookmarked",
        "hs_comments_hearted_count": "hs_comments_hearted_count",
        "hs_default_comments_count": "hs_default_comments_count",
        "hs_gameInvite_popup_seen": "hs_game_invite_popup_seen",
        "hs_gi_comment_sm_clicked": "hs_gi_comment_sm_clicked",
        "hs_gi_gameNames": "hs_gi_game_names",
        "hs_gi_oc_clicked_count": "hs_gi_oc_clicked_count",
        "hs_gi_playerProfile": "hs_gi_player_profile",
        "hs_gi_timeSpent": "hs_gi_time_spent",
        "hs_h2h_page_player_profile": "hs_h2h_page_player_profile",
        "hs_oc_CodeScrSec": "hs_oc_code_scr_sec",
        "hs_oc_GenCt": "hs_oc_gen_ct",
        "hs_oc_cancelled": "hs_oc_cancelled",
        "hs_oc_details_popup_seen": "hs_oc_details_popup_seen",
        "hs_oc_game_genCtV2": "hs_oc_game_gen_ct_v2",
        "hs_oc_timeout": "hs_oc_timeout",
        "hs_scroll_used%": "hs_scroll_used_percent",
        "hs_search_1": "hs_search_1",
        "hs_time_spent": "hs_time_spent",
        "hs_typed_comments_count": "hs_typed_comments_count",
        "isDebugMenuEnabled": "is_debug_menu_enabled",
        "isInternal": "is_internal",
        "is_video": "is_video",
        "last_filter": "last_filter",
        "media_source": "media_source",
        "Country": "country",
        "Mixpanel Library": "mixpanel_library",
        "Time Processed (UTC)": "time_processed_utc",
        "netConnectedCount": "net_connected_count",
        "oc_id": "oc_id",
        "player_level": "player_level",
        "rScore": "r_score",
        "scene_load_time": "scene_load_time",
        "session_id": "session_id",
        "total_milestones": "total_milestones",
        "NoInternetPopupShown": "no_internet_popup_shown",
        "app_minimised_count": "app_minimised_count",
        "endScreen_type": "end_screen_type",
        "exit_reason": "exit_reason",
        "frame_rate": "frame_rate",
        "funnel_step": "funnel_step",
        "game_ftue_data": "game_ftue_data",
        "game_levels": "game_levels",
        "game_value1": "game_value1",
        "game_value2": "game_value2",
        "game_value3": "game_value3",
        "game_value4": "game_value4",
        "game_value5": "game_value5",
        "game_value6": "game_value6",
        "game_value7": "game_value7",
        "game_value8": "game_value8",
        "game_value9": "game_value9",
        "game_value10": "game_value10",
        "game_value11": "game_value11",
        "game_value12": "game_value12",
        "game_value13": "game_value13",
        "game_value14": "game_value14",
        "game_value15": "game_value15",
        "game_value16": "game_value16",
        "game_value17": "game_value17",
        "game_value18": "game_value18",
        "game_value19": "game_value19",
        "game_value20": "game_value20",
        "inLeaderBoardOpened": "in_leaderboard_opened",
        "items_used": "items_used",
        "leaderboard_place": "leaderboard_place",
        "level_load_times": "level_load_times",
        "masterApiRequestTime": "master_api_request_time",
        "score_earned": "score_earned",
        "session_delta_milestone": "session_delta_milestone",
        "share_Count": "share_count",
        "share_IDs": "share_ids",
        "shared_Feed": "shared_feed",
        "shared_WA": "shared_wa",
        "sm_popup_seen": "sm_popup_seen",
        "tc1Spent": "tc1_spent",
        "tc1_badge": "tc1_badge",
        "tc1_earned": "tc1_earned",
        "terraApiDelayed": "terra_api_delayed",
        "time_spent": "time_spent",
        "xp_earned": "xp_earned",
        "multiplayerSource": "multiplayer_source",
        "netCodeGenCt": "net_code_gen_ct",
        "netCodeSuccCt": "net_code_succ_ct",
        "netCodeTypeCt": "net_code_type_ct",
        "netConnectedPlayers": "net_connected_players",
        "netLobbyInviteChallengeCt": "net_lobby_invite_challenge_ct",
        "oc_ConnectedPlayers": "oc_connected_players",
        "AnalyticsVersion": "analytics_version",
        "MVPVersion": "mvp_version",
        "exp_homescreen_tabs": "exp_homescreen_tabs",
        "is_maximise": "is_maximise",
        "api_data_sent": "api_data_sent",
        "api_error": "api_error",
        "api_result": "api_result",
        "api_server_response": "api_server_response",
        "api_url": "api_url",
        "Anonymous ID": "anonymous_id",
        "Identified ID": "identified_id",
        "Manufacturer": "manufacturer",
        "share_feed_count": "share_feed_count",
        "deep_link_value": "deep_link_value",
        "hs_ai_avatar_style": "hs_ai_avatar_style",
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
        """
        if raw is None:
            return None

        # If already a datetime
        if isinstance(raw, datetime):
            dt = raw
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()

        # If it's a string, try ISO parse first
        if isinstance(raw, str):
            raw_str = raw.strip()
            try:
                if raw_str.endswith("Z"):
                    iso = raw_str.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(iso)
                    return dt.astimezone(timezone.utc).isoformat()
                else:
                    dt = datetime.fromisoformat(raw_str)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc).isoformat()
            except Exception:
                try:
                    if raw_str.isdigit():
                        num = float(raw_str)
                    else:
                        num = float(raw_str)
                except Exception:
                    return None
        else:
            try:
                num = float(raw)
            except Exception:
                return None

        # Determine unit by magnitude
        if math.isfinite(num):
            absnum = abs(num)
            if absnum >= 1e14:
                ts_seconds = num / 1e6
            elif absnum >= 1e11:
                ts_seconds = num / 1e3
            elif absnum >= 1e9:
                ts_seconds = num
            else:
                return None
        else:
            return None

        # Validate reasonable timestamp range
        if ts_seconds < 0 or ts_seconds > 253402300799:
            return None

        try:
            dt = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
            return dt.isoformat()
        except Exception:
            return None

    def parse_boolean(self, value):
        """Convert various boolean representations to Python bool or None"""
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            value_lower = value.strip().lower()
            if value_lower in ("true", "1", "yes"):
                return True
            elif value_lower in ("false", "0", "no"):
                return False
        return None

    def parse_integer(self, value):
        """Convert value to integer or None"""
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def parse_float(self, value):
        """Convert value to float or None"""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def build_bq_row_from_csv(self, row_dict: dict) -> dict:
        """
        Build a BigQuery row from CSV data (dict with CSV column headers as keys)
        """
        cleaned_row = {}
        properties = {}

        # Map CSV column names to BigQuery field names
        for csv_column, value in row_dict.items():
            # Skip empty values
            if value == "" or value is None:
                continue

            # Get the mapped field name (snake_case)
            bq_field = self.FIELD_MAPPINGS.get(csv_column, csv_column.lower().replace(" ", "_"))

            # Handle timestamp fields
            if bq_field in ["time", "registration_date", "event_time_stamp"]:
                converted = self.to_utc_iso(value)
                value = converted

            # Handle boolean fields
            elif bq_field in ["wifi", "has_gyro", "is_editor", "config_ge_enabled", 
                             "is_debug_menu_enabled", "is_internal", "is_video"]:
                value = self.parse_boolean(value)

            # Handle integer fields
            elif bq_field in ["screen_dpi", "screen_height", "screen_width", "active_days",
                            "cpu_cores", "days_since_install", "max_streak", "ram", "user_age",
                            "vram", "weeks_since_install", "app_game_count", "cycle_id",
                            "day_of_game", "friends_network", "game_entry_count", "game_position",
                            "api_timestamp", "time_processed_utc", "net_connected_count",
                            "player_level", "session_id", "total_milestones", "no_internet_popup_shown",
                            "app_minimised_count", "frame_rate", "funnel_step", "leaderboard_place",
                            "master_api_request_time", "score_earned", "session_delta_milestone",
                            "share_count", "time_spent", "xp_earned", "net_code_gen_ct",
                            "net_code_succ_ct", "net_code_type_ct", "net_lobby_invite_challenge_ct",
                            "oc_connected_players", "analytics_version", "share_feed_count"]:
                value = self.parse_integer(value)

            # Handle float fields
            elif bq_field in ["game_load_time", "scene_load_time"]:
                value = self.parse_float(value)

            # Add to appropriate section
            if bq_field in self.TOP_LEVEL_FIELDS_TERRA:
                if bq_field == "name":
                    # Event Name maps to 'name' at top level
                    cleaned_row["name"] = value
                elif bq_field == "distinct_id":
                    cleaned_row["distinct_id"] = value
                elif bq_field == "time":
                    cleaned_row["time"] = value
            elif bq_field in self.PROPERTIES_FIELDS_TERRA:
                properties[bq_field] = value

        # Set properties
        cleaned_row["properties"] = properties
        cleaned_row["dataset"] = "terra"

        return cleaned_row

    def upload_events(self, csv_file_path: str, table_id: str, batch_size: int = 500):
        """
        Upload events from CSV file to BigQuery
        """
        import csv
        
        rows_to_insert = []
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for i, row in enumerate(reader, start=1):
                try:
                    bq_row = self.build_bq_row_from_csv(row)
                    
                    # Ensure time is set at top level
                    if not bq_row.get("time"):
                        bq_row["time"] = self.to_utc_iso(row.get("Time"))
                    
                    # Ensure distinct_id is set
                    if not bq_row.get("distinct_id"):
                        bq_row["distinct_id"] = row.get("Distinct ID")
                    
                    # Ensure name (event name) is set
                    if not bq_row.get("name"):
                        bq_row["name"] = row.get("Event Name")
                    
                    rows_to_insert.append(bq_row)
                    
                    if len(rows_to_insert) >= batch_size:
                        errors = self.client.insert_rows_json(table_id, rows_to_insert)
                        if errors:
                            print(f"❌ Error inserting rows at batch ending {i}: {errors}")
                        else:
                            print(f"✅ Uploaded {i} rows...")
                        rows_to_insert = []
                        
                except Exception as e:
                    print(f"⚠️ Error processing row {i}: {e}")
                    continue

        # Upload remaining rows
        if rows_to_insert:
            errors = self.client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                print(f"❌ Error inserting final rows: {errors}")
            else:
                print(f"✅ Uploaded {i} rows total.")
        
        print("🎯 Upload complete!")



bigquery_client = bigquery.Client()

bigQueryServices = BigQueryServices(bigquery_client)
terraDataBigQueryServices = TerraDataBigQueryServices(bigquery_client)

events = mixPanelServices.fetch_events("2024-12-10", "2024-12-12")
# terraDataBigQueryServices.
# bigQueryServices.upload_events(events, "ai-analytics-463910.Ripple.Event")
