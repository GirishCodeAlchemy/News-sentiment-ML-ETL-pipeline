import json
import time

from hdfs import InsecureClient
from hdfs.util import HdfsError
from kafka import KafkaConsumer
from loguru import logger

logger.add("consumer.log", rotation="500 MB", retention="30 days", level="DEBUG")

class KafkaConsumerToHDFS:
    def __init__(self, config_path='config.json'):
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)

        kafka_config = config['kafka']
        hdfs_config = config['hdfs']

        self.kafka_bootstrap_servers = kafka_config['bootstrap_servers']
        self.kafka_topic = kafka_config['topic']
        self.hdfs_url = hdfs_config['url']
        self.hdfs_path = hdfs_config['path']
        self.hdfs_file_path = f"{self.hdfs_path}/{hdfs_config['file_name']}"

        # Create KafkaConsumer instance
        self.consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id='news_consumer_group',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        # Create HDFS client
        self.hdfs_client = InsecureClient(self.hdfs_url, user="root")

        # Check if HDFS path exists, create it if not
        self.create_hdfs_path()

    def create_hdfs_path(self):
        try:
            # Check if HDFS path exists
            self.hdfs_client.status(self.hdfs_path)
        except HdfsError as e:
            logger.error(f"HDFS Error: {e}")
            logger.info(f"Creating the HDFS path: {self.hdfs_path}")
            # Create HDFS path if it doesn't exist
            self.hdfs_client.makedirs(self.hdfs_path)

    def write_to_hdfs(self, data):
        # Append data to an HDFS file
        logger.info(f"Writing consumer data to HDFS file: hdfs:///{self.hdfs_file_path}")
        try:
            if not self.hdfs_client.status(self.hdfs_file_path, strict=False):
                # If file doesn't exist, create it
                with self.hdfs_client.write(
                  self.hdfs_file_path, encoding='utf-8') as writer:
                    writer.write(data + '\n')
            else:
                # If file exists, append to it
                with self.hdfs_client.write(
                  self.hdfs_file_path, encoding='utf-8', append=True) as writer:
                    writer.write(data + '\n')
        except HdfsError as e:
            logger.error(f"HDFS Error: {e}")

    def run(self):
        try:
            for message in self.consumer:
                news_data = message.value
                logger.info(f"Received a message: {json.dumps(news_data, indent=2)}")
                self.write_to_hdfs(json.dumps(news_data))

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == '__main__':
    consumer_to_hdfs = KafkaConsumerToHDFS()
    consumer_to_hdfs.run()
