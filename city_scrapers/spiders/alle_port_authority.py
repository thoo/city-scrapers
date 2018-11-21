# -*- coding: utf-8 -*-
from city_scrapers.spider import Spider
from lxml import html
import unicodedata
from datetime import datetime, timedelta
from dateutil.parser import parse
from city_scrapers.constants import BOARD, COMMITTEE

class AllePortAuthoritySpider(Spider):
    name = 'alle_port_authority'
    agency_name = 'Port Authority of Allegheny County'
    timezone = 'America/New_York'
    allowed_domains = ['www.portauthority.org']
    start_urls = ['https://www.portauthority.org/paac/CompanyInfoProjects/BoardofDirectors/MeetingAgendasResolutions.aspx']
    event_year = str(datetime.now().year)

    def _build_datatable(self,response):
        alist_tbody = (response
                 .xpath('//table[1]/tbody//td')
                 .extract()
                )

        atable=[]
        arow=[]

        for item in alist_tbody:
            tree = html.fragment_fromstring(item)
            #pdb.set_trace()
            text = tree.text_content()

            url = tree.xpath('//a/@href')
            find_att_b = tree.xpath('//b/text()|//strong/text()')
            if len(find_att_b)>=1:
                continue;
            if url:
                arow.append('{text}: {url}'
                            .format(text=text, url=url[0]))
            else:
                arow.append('{text}'.format(text=unicodedata.normalize("NFKD", text)))
            if len(arow) == 6:
                atable.append(arow)
                arow = []
        #import pdb;pdb.set_trace()
        return atable

    def parse(self, response):
        """
        `parse` should always `yield` a dict that follows a modified
        OCD event schema (docs/_docs/05-development.md#event-schema)

        Change the `_parse_id`, `_parse_name`, etc methods to fit your scraping
        needs.
        """
        atable = self._build_datatable(response)
        #import pdb;pdb.set_trace()
        for row in atable:
            data = {
                'timezone': self.timezone,
                '_type': 'event',
                'name': self._parse_name(row),
                'event_description': self._parse_description(row),
                'classification': self._parse_classification(row),
                'start': self._parse_start(row),
                'end': self._parse_end(row),
                'all_day': self._parse_all_day(row),
                'location': self._parse_location(row),
                'documents': self._parse_documents(row),
                'sources': self._parse_sources(row),
            }

            data['status'] = self._generate_status(data)
            data['id'] = self._generate_id(data)

            yield data

        # self._parse_next(response) yields more responses to parse if necessary.
        # uncomment to find a "next" url
        # yield self._parse_next(response)


    def _parse_name(self, item):
        """
        Parse or generate event name.
        """
        return item[0]

    def _parse_description(self, item):
        """
        Parse or generate event description.
        """
        return ''

    def _parse_classification(self, item):
        """
        Differentiate board meetings from public hearings.
        """
        meeting_title = item[0].lower()
        if 'committee' in meeting_title:
            return COMMITTEE
        return BOARD

    def _parse_start_datetime(self, item):
        """
        Parse the start datetime.
        """
        if 'cancel' in item[2].lower():
            return ''

        if not item[1].strip():
            if 'stakeholder' in item[0].lower():
                time = '8:30 a.m.'
            if 'performance oversight' in item[0].lower():
                time = '9:00 a.m.'
            else:
                time = '9:30 a.m.'
        else:
             time = item[1]
        date = ('{year} {date}'
                .format(year=self.event_year, date=item[2]))

        time_string = '{0} {1}'.format(date, time)
        return (parse(time_string))

    def _parse_start(self, item):
        datetime_obj = self._parse_start_datetime(item)
        if not datetime_obj:
            return ''
        return {
            'date': datetime_obj.date(),
            'time': datetime_obj.time(),
            'note': ''
        }

    def _parse_end(self, item):
        """
        No end date listed. Estimate 3 hours after start time.
        """
        datetime_obj = self._parse_start_datetime(item)
        if not datetime_obj:
            return ''
        return {
            'date':  datetime_obj.date(),
            'time': ((datetime_obj + timedelta(hours=3))
                     .time()),
            'note': 'Estimated 3 hours after start time'
        }

    def _parse_all_day(self, item):
        """
        Parse or generate all-day status. Defaults to False.
        """
        return False

    def _parse_location(self, item):
        """
        Parse or generate location. Latitude and longitude can be
        left blank and will be geocoded later.
        """
        return {
            'address': ('Neal H. Holmes Board Room, '
                        '345 Sixth Avenue, Fifth Floor, '
                        'Pittsburgh, PA 15222-2527'),
            'name': '',
            'neighborhood': '',
        }

    def _parse_documents(self, item):
        """
        Parse or generate documents.
        """
        documents = []

        details = item[5]
        if details.startswith('Minutes: http'):
            documents.append({
                'note': 'Minutes',
                'url': details.split(' ')[-1]
            })

        agenda = item[3]
        if agenda.startswith('Agenda: http'):
            documents.append({
                'note': 'Agenda',
                'url': agenda.split(' ')[-1]
            })

        return documents

    def _parse_sources(self, item):
        """
        Parse or generate sources.
        """
        return [{'url': self.start_urls[0], 'note': ''}]