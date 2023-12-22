import json
import re
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger
from newsapi import NewsApiClient, newsapi_exception

logger.add("producer.log", rotation="500 MB", retention="30 days", level="DEBUG")

class NewsAPIKafkaProducer:
    def __init__(self, config_path='config.json'):
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)

        kafka_config = config['kafka']
        newsapi_config = config['newsapi']

        self.kafka_bootstrap_servers = kafka_config['bootstrap_servers']
        self.kafka_topic = kafka_config['topic']

        self.newsapi_key = newsapi_config['key']
        self.sources = newsapi_config['source']
        self.api_page_size = newsapi_config["api_page_size"]

        # Create KafkaProducer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Create NewsApiClient
        self.newsapi = NewsApiClient(api_key=self.newsapi_key)

    def fetch_news(self, keyword, page=1):
        news_data = []
        try:
            response = self.newsapi.get_everything(
                q=keyword, language='en', sources=self.sources,
                page=page, page_size=self.api_page_size
            )
            news_data = self.fetch_articles(response, news_data)
        except newsapi_exception.NewsAPIException as e:
            logger.error(f"News API Error: {e}")
        return news_data

    def fetch_articles(self, response, news_data):
        articles = response.get('articles', [])
        for article in articles:
            if self.clean_text(article.get('title')) != "REMOVED":
                news_data.append(self.clean_article_data(article))
            else:
                logger.warning(f"Article with title 'REMOVED' encountered. Skipping the article: {article}")
        return news_data

    def clean_article_data(self, article):
        data = {
            'source': article.get('source', {"id": "unknown", "name":"Unknown"}),
            'author': self.clean_author(article.get('author', "Unknown")),
            'title': self.clean_text(article.get('title', "Unknown")),
            'description': self.clean_text(article.get('description', "Unknown")),
            'url': article.get('url'),
            'url_to_image': article.get('urlToImage'),
            'published_at': article.get('publishedAt', "1970-01-01T00:00:00Z"),
            'content': self.clean_text(article.get('content',"Unknown")),
        }
        logger.debug(f"Cleaned article data: {data}")
        return data

    def clean_text(self, text):
        if text:
            cleaned_text = re.sub(r'\s*\[.*chars\]$', '', text.replace('[Removed]', 'REMOVED').replace(' …', '').replace('\r', ''))
            return cleaned_text.replace('\n', ' ').strip()
        return "Unknown"

    def clean_author(self, text):
        # Extract the last part of the URL, example: https://www.facebook.com/bbcnews --> bbcnews
        if text:
            match = re.search(r'https?://www\..*\.com/(?P<author>[^\/\s]+)', text)
            if match:
                author = match.group("author")
                logger.debug(f"Cleaning the author from the article, Original author: {text}, Extracted author: {author}")
                return author
            return text
        else:
            return "Unknown"

    def produce_to_kafka(self, news_data):
        for news_item in news_data:
            try:
                self.producer.send(self.kafka_topic, value=news_item)
            except KafkaError as e:
                logger.error(f"Failed to produce message: {e}, news_data: {news_data}")

    def run(self):
        while True:
            user_input = input("\nEnter a News keyword to fetch the article from NewsAPI (or 'exit' to quit): ")
            if user_input.lower() == 'exit':
                break
            print("\n")
            logger.info(f"Fetching news for keyword '{user_input}' from NewsAPI")
            news_data = self.fetch_news(user_input)
            if len(news_data) == 0:
                logger.warning(f"No articles found for keyword '{user_input}'. Try a different keyword.")
            else:
                logger.info(f"Fetched latest {len(news_data)} articles for keyword '{user_input}' from NewsAPI")
                self.produce_to_kafka(news_data)
                logger.info(f"Successfully posted latest {len(news_data)} articles for keyword '{user_input}' from NewsAPI to Kafka Broker")
            print("*"*100)
            print("\n")
            time.sleep(5)


if __name__ == '__main__':
    news_producer = NewsAPIKafkaProducer()
    news_producer.run()
