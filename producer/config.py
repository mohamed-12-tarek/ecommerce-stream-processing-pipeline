NUM_USERS = 10_000
NUM_PRODUCTS = 5_000

MIN_EVENT_DELAY = 0.1
MAX_EVENT_DELAY = 0.8

MIN_SESSION_ACTIONS = 3
MAX_SESSION_ACTIONS = 15

# The producer runs in real time, but event timestamps use a simulated
# clock so the dataset can represent many days while the producer is running.
SIMULATION_START_DATE = "2026-08-18T00:00:00+00:00"
MIN_SESSION_GAP_MINUTES = 15
MAX_SESSION_GAP_MINUTES = 240

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ecommerce-events"

HDFS_CONSUMER_GROUP = "hdfs-writer"
HDFS_LOCALHOST = "http://localhost:9870"
HDFS_BASE_PATH = "/mindmetrics/task5/data/ecommerce/events"

# Keep Kafka polling and group membership tolerant of HDFS I/O latency.
HDFS_BATCH_SIZE = 500
HDFS_POLL_TIMEOUT_MS = 1000
HDFS_MAX_POLL_INTERVAL_MS = 300_000
HDFS_SESSION_TIMEOUT_MS = 60_000
HDFS_HEARTBEAT_INTERVAL_MS = 20_000
HDFS_REQUEST_TIMEOUT_MS = 60_000
HDFS_RETRY_BACKOFF_MS = 1_000

KAFKA_GROUP_ID_FLINK = "realtime-pipeline"

CATEGORIES = [
    "electronics",
    "computers",
    "smartphones",
    "home",
    "fashion",
    "beauty",
    "sports",
    "books",
    "toys",
    "grocery",
]
