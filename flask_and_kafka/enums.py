from enum import Enum

class KafkaStatusEnum(Enum):
    success = "Success"
    failed = "Failed"
    retry = "Retry"

class FailedIssueTypeEnum(str, Enum):
    consumer = "consumer"

class KafkaFailTopicEnum(Enum):
    failed_topic = "failed"

class KafkaRetryTopicEnum(Enum):
    """
    Note: Keep retry_topics_attempt_time_map structure in any customization
    """
    retry_5m_topic = "retry_5m"
    retry_10m_topic = "retry_10m"
    retry_15m_topic = "retry_15m"
    retry_30m_topic = "retry_30m"
    retry_1h_topic = "retry_1h"
    retry_topics_attempt_time_map = {
        0: (retry_5m_topic, 60 * 5),
        1: (retry_10m_topic, 60 * 10),
        2: (retry_15m_topic, 60 * 15),
        3: (retry_30m_topic, 60 * 30),
        4: (retry_1h_topic, 60 * 60),
    }