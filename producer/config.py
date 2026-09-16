NUM_USERS = 10_000
NUM_PRODUCTS = 5_000

MIN_EVENT_DELAY = 0.1
MAX_EVENT_DELAY = 0.8

MIN_SESSION_ACTIONS = 3
MAX_SESSION_ACTIONS = 15

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ecommerce-events"

HDFS_CONSUMER_GROUP = "hdfs-writer"
HDFS_LOCALHOST = "http://localhost:9870"
HDFS_BASE_PATH = "/mindmetrics/task5/data/ecommerce/events"
HDFS_BATCH_SIZE = 500
HDFS_POLL_TIMEOUT_MS = 1000

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