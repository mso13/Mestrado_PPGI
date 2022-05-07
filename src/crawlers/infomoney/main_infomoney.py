import os
import re
import json
import time
import scrapy
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest

class InfoMoneySpider(scrapy.Spider):

    name = "infomoney_spider"

    def start_requests(self):

        data = list()

        # URL to POST request
        url_base = 'https://www.infomoney.com.br/?infinity=scrolling'

        # Set number of pages to download on range(1, x)
        for i in range(1, 1500):

            params = dict()
            params['action'] = 'infinite_scroll'
            params['page'] = str(i)
            params['order'] = 'DESC'

            data.append(params)

        for form_data in data:
            time.sleep(3)
            yield FormRequest(
                url=url_base, 
                callback=self.parse_front, 
                formdata=form_data,
                cb_kwargs=dict(metadata=form_data), 
                dont_filter=True, 
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded', 
                    'charset':'UTF-8'
                }
            )

    def parse_front(self, response, metadata):

        # Narrow in on the news cards (10 cards per page)
        news_cards = response.css('div')

        # Direct to news links
        news_links = news_cards.xpath('./a/@href')

        # Extract the links (as a list of strings)
        links_to_follow = news_links.extract()

        # Follow the links to the next parser
        for url in links_to_follow:
            time.sleep(1)
            yield response.follow(
                        url=url.replace('\\', ''), 
                        callback=self.parse_pages
                    )

    def parse_pages(self, response):

        # Direct to the news title text
        # news_title = response.xpath('//h1[contains(@class,"page-title-1")]/text()')
        
        news_title = response.xpath('//h1[contains(@class,"typography__display--2")]/text()')

        # Extract and clean the news title text
        news_title_ext = news_title.extract_first().strip()

        # Direct to the news date
        news_date = response.xpath('//div[contains(@class, "single__author-info")]//time[contains(@class, "entry-date published")]/@datetime')

        # Extract the news date
        news_date_ext = news_date.extract_first().strip()
        news_date_ext = news_date_ext.replace('T', ' ')
        news_date_ext = news_date_ext.replace('-03:00', '')

        # Direct to the news full text
        # news_full_text = response.xpath('//div[contains(@class, "article-content")]//p//text()')

        # Extract the news full text
        # separator = ''

        # full_string_text = separator.join(news_full_text.extract())

        # # Replacing special character
        # full_string_text = full_string_text.replace(' ', ' ')

        # full_string_text = re.sub(' +', ' ', full_string_text)

        # news_full_text_ext = full_string_text.strip()

        # Direct to the news tags
        news_tags = response.css('div.single__tag-list ul li a ::text')

        # Extract the news tags
        news_tags_ext = [t.strip() for t in news_tags.extract()]

        # Extract main topic from URL
        full_url = response.url

        m = re.search('https://www.infomoney.com.br/(.+?)/', full_url)
        if m:
            main_topic = m.group(1)
        else:
            main_topic = ''

        # Save all the data collected
        results_dict = dict()

        today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        results_dict['topic'] = main_topic
        results_dict['title'] = news_title_ext
        results_dict['date'] = news_date_ext
        results_dict['search_date'] = today
        results_dict['link'] = response.url
        results_dict['tags'] = news_tags_ext

        results_list.append(results_dict)


if __name__ == '__main__':

    THIS_DIR = os.path.dirname(os.path.abspath(__file__))

    filename = 'infomoney'

    # List to save the data collected
    results_list = list()

    # Initiate a CrawlerProcess
    process = CrawlerProcess()

    # Tell the process which spider to use
    process.crawl(InfoMoneySpider)

    # Start the crawling process
    process.start()

    # Save the list of dicts
    with open(f'./results/{filename}-results.json', 'w', encoding='utf8') as f:
        json.dump(results_list, f, ensure_ascii=False)