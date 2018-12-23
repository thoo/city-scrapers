# -*- coding: utf-8 -*-
from datetime import datetime
from dateutil.parser import parse
from lxml import html,etree
from city_scrapers.constants import BOARD, FORUM
from city_scrapers.spider import Spider
from scrapy.http import FormRequest
from scrapy import Request
import json

class PaDevelopmentSpider(Spider):
    custom_settings = {'ROBOTSTXT_OBEY': False}

    name = 'pa_development'
    agency_name = 'Department of Community & Economic Development'
    timezone = 'America/Chicago'
    allowed_domains = ['dced.pa.gov']
    start_urls = ['https://dced.pa.gov/events/']

    def start_requests(self):
        url = 'https://dced.pa.gov/wp-admin/admin-ajax.php'
        payload = {'action': 'tribe_list',
                   'tribe_paged': '2',
                   'tribe_event_display': 'list',
                   'featured': 'false'}
        yield FormRequest(url, formdata=payload)


    def parse(self, response):
        """
        `parse` should always `yield` a dict that follows a modified
        OCD event schema (docs/_docs/05-development.md#event-schema)

        Change the `_parse_id`, `_parse_name`, etc methods to fit your scraping
        needs.
        """
        data = json.loads(response.body)
        root=etree.HTML(data['html'])
        urls = root.xpath('//a[@class="tribe-events-read-more"]/@href')

        for url in urls:
            yield FormRequest(url,callback=self.parse_schedule)


    def parse_schedule(self, response):
        import pdb;pdb.set_trace()
        description = response.xpath('//*[@class="tribe-events-event-categories"]//text()').extract()[0]

        data = {
                '_type': 'event',
                'name': description,
                'event_description': description,
                'classification': self._parse_classification(response),
                'start': self._parse_start(response),
                'end': self._parse_end(response),
                'all_day': self._parse_all_day(response),
                'location': self._parse_location(response),
                'documents': '',
                'sources': self._parse_sources(response)
            }

        data['status'] = self._generate_status(data, description)
        data['id'] = self._generate_id(data)

        yield data
        # for item in response.css('.eventspage'):
        #     data = {
        #         '_type': 'event',
        #         'name': self._parse_name(item),
        #         'event_description': self._parse_description(item),
        #         'classification': self._parse_classification(item),
        #         'start': self._parse_start(item),
        #         'end': self._parse_end(item),
        #         'all_day': self._parse_all_day(item),
        #         'location': self._parse_location(item),
        #         'documents': self._parse_documents(item),
        #         'sources': self._parse_sources(item),
        #     }

        #     data['status'] = self._generate_status(data)
        #     data['id'] = self._generate_id(data)

        #     yield data

        # self._parse_next(response) yields more responses to parse if necessary.
        # uncomment to find a "next" url
        # yield self._parse_next(response)

    def _parse_next(self, response):
        """
        Get next page. You must add logic to `next_url` and
        return a scrapy request.
        """
        next_url = None  # What is next URL?
        return scrapy.Request(next_url, callback=self.parse)

    def _parse_name(self, item):
        """
        Parse or generate event name.
        """
        return ''

    def _parse_description(self, item):
        """
        Parse or generate event description.
        """
        description = response.xpath('//*[@class="tribe-events-event-categories"]//text()').extract()[0]

        if 'board' in description.lower():
            classification = BOARD
        elif 'forum' in text_body.lower():
            classification = FORUM
        elif 'workshop' in text_body.lower():
            classification = 'WORKSHOP'

        return classification

    def _parse_classification(self, item):
        """
        Parse or generate classification (e.g. public health, education, etc).
        """
        return ''

    def _parse_start_datetime(self, response):
        """
        Parse the start datetime.
        """
        start_time = response.xpath('//*[@class="tribe-event-date-start"]/text()').extract()[0]
        return parse(start_time.replace('@',''))

    def _parse_start(self, item):
        """
        Parse start date and time.
        """
        datetime_obj = self._parse_start_datetime(item)
        if not datetime_obj:
            return ''
        return {'date': datetime_obj.date(), 'time': datetime_obj.time(), 'note': ''}

    def _parse_end(self, item):
        """
        Parse end date and time.
        """

        datetime_obj = self._parse_start_datetime(item)
        end_time = item.xpath('//*[@class="tribe-event-time"]/text()').extract()[0]
        if not datetime_obj:
            return ''
        return {
            'date': datetime_obj.date(),
            'time': end_time
        }

    def _parse_all_day(self, item):
        """
        Parse or generate all-day status. Defaults to False.
        """
        return False

    def _parse_location(self, response):
        """
        Parse or generate location. Latitude and longitude can be
        left blank and will be geocoded later.
        """
        building = response.xpath('//p[@class="venuefix"]/text()').extract()[0]
        loc = " ".join(response.xpath('//*[@class="tribe-address"]//text()').extract())
        loc = loc.replace('\n','').replace('\t','').replace('United States','')
        loc = ' '.join(loc.split())
        address = '{bld,loc}'.format(bld=building, loc=loc)
        return {
            'address': address,
            'name': '',
            'neighborhood': '',
        }


    def _parse_sources(self, response):
        """
        Parse or generate sources.
        """
        return [{'url': response.url, 'note': ''}]
