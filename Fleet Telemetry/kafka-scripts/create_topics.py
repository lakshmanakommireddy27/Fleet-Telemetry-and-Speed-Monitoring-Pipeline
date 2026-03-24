from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time

# Chờ cho Kafka khởi động hoàn toàn
time.sleep(30)

admin_client = KafkaAdminClient(
    bootstrap_servers="kafka:9092",
    client_id='test'
)

# Định nghĩa các topic bạn muốn tạo
topics = [
    NewTopic(name="vehicle-data", num_partitions=3, replication_factor=1),
]

# Tạo các topic
try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("Topics created successfully")
except TopicAlreadyExistsError:
    print("Topics already exist")
