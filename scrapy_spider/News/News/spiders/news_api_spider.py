import scrapy
import requests
import json
from datetime import datetime


class NewsAPISpider(scrapy.Spider):
    name = "news_api_spider"
    allowed_domains = ["newsapi.org"]
    start_urls = ["https://newsapi.org/v2/everything"]

    api_key = "API_KEY"  # Replace with your NewsAPI key
    search_keywords = ["technology", "finance"]

    def start_requests(self):
        for keyword in self.search_keywords:
            url = f"https://newsapi.org/v2/everything?q={keyword}&apiKey={self.api_key}&sources=bbc-news&language=en"
            response = requests.get(url)
            print(response)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # Parse the JSON response from the NewsAPI
        data = response.json()

        raw_data_path = "raw_json_data.json"

        with open(raw_data_path, "w") as f:
            json.dump(data, f, indent=4)

        if data.get("status") == "ok":
            for article in data["articles"]:
                self.log(f"Yielding article: {article['title']}")
                yield {
                    "source": article.get("source", {}).get("name").strip(),
                    "author": article.get("author"),
                    "title": article.get("title"),
                    "description": article.get("description"),
                    "url": article.get("url"),
                    "publishedAt": article.get("publishedAt"),
                    "content": article.get("content"),
                    "dateFetched": datetime.now(),
                }
        else:
            self.log(f"Error fetching data: {data.get('message')}")
