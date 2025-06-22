import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCENARIO_EVENTS_TOPIC = os.getenv("SCENARIO_EVENTS_TOPIC", "scenario_events")
SCENARIO_COMMANDS_TOPIC = os.getenv("SCENARIO_COMMANDS_TOPIC", "scenario_commands")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "default_group")
