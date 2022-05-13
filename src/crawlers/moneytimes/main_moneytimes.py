import os
import re
import json
import time
import scrapy
from datetime import datetime, timedelta
from scrapy.crawler import CrawlerProcess


class MoneyTimesSpider(scrapy.Spider):

    name = "moneytimes_spider"

    def start_requests(self):

        # Set number of pages to download on range(1, x)
        urls = ['https://www.moneytimes.com.br/tag/petrobras/page/%s' % i for i in range(1, 2000)]

        for url in urls:
            time.sleep(3)
            yield scrapy.Request( 
                        url=url, 
                        callback=self.parse_front 
            )

    def parse_front(self, response):

        # Narrow in on the news cards (10 cards per page)
        news_items = response.css('div.news-item ')

        # Direct to news links
        news_links = news_items.css('h2.news-item__title > a::attr(href)')

        # Extract the links (as a list of strings)
        links_to_follow = news_links.extract()

        # Follow the links to the next parser
        for url in links_to_follow:
            time.sleep(1)
            yield response.follow(
                            url=url,
                            callback=self.parse_pages
                        )


    def parse_pages(self, response):

        # Direct to the news main topic
        news_topic = response.xpath('//div[contains(@class, "single__category")]/a/text()')

        # Extract the news main topic
        news_topic_ext = news_topic.extract_first().strip()

        # Direct to the news title text
        news_title = response.xpath('//h1[contains(@class,"single__title")]/text()')

        # Extract and clean the news title text
        news_title_ext = news_title.extract_first().strip()

        # Direct to the news date
        news_date = response.css('div.single-meta > div.single-meta__date::text')

        # Extract the news date
        news_date_ext = news_date.extract_first()

        # Direct to the news full text
        news_full_text = response.css('div.single__text p ::text')

        # Extract the news full text
        separator = ''

        full_string_text = separator.join(news_full_text.extract())

        # Replacing special character
        full_string_text = full_string_text.replace(' ', ' ')

        full_string_text = re.sub(' +', ' ', full_string_text)

        news_full_text_ext = full_string_text.strip()

        # Direct to the news tags
        news_tags = response.css('div.single__tags a ::text')

        # Extract the news tags
        news_tags_ext = [t.strip() for t in news_tags.extract()]

        # Save all the data collected
        results_dict = dict()

        today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        results_dict['topic'] = news_topic_ext
        results_dict['title'] = news_title_ext
        results_dict['date'] = news_date_ext
        results_dict['search_date'] = today
        results_dict['link'] = response.url
        results_dict['tags'] = news_tags_ext

        results_list.append(results_dict)


if __name__ == '__main__':

    ticker = 'petr4'

    filename = 'moneytimes'

    # List to save the data collected
    results_list = list()

    # Initiate a CrawlerProcess
    process = CrawlerProcess()

    # Tell the process which spider to use
    process.crawl(MoneyTimesSpider)

    # Start the crawling process
    process.start()

    # Save the list of dicts
    with open(f'./results/{filename}-{ticker}.json', 'w', encoding='utf8') as f:
        json.dump(results_list, f, ensure_ascii=False)