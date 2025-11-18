"""
Central Ingest Module

This module handles the central ingestion of data from various sources,
processes it, and stores it in the appropriate databases.

Functions:
- Subscripes to MQTT topics for incoming data.
- decompresses gzip data sent from edge collectors.
- parse JSON payloads.
- deduplicarte entries based on unique identifiers (panel_id and timestamp_utc).
- write processed data to InfluxDB or fallback JSON file if DB is unavailable.
"""