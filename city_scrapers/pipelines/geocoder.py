"""
Geocoder.
"""
import geocoder
import requests
import usaddress
import re
import os
from airtable import Airtable
from random import randint

AIRTABLE_BASE_KEY = os.environ.get('CITY_SCRAPERS_AIRTABLE_BASE_KEY')
AIRTABLE_GEOCODE_TABLE = os.environ.get('CITY_SCRAPERS_AIRTABLE_GEOCODE_TABLE')
TAMU_API_KEY = os.environ.get('CITY_SCRAPERS_TAMU_API_KEY')


class GeocoderPipeline(object):
    def __init__(self, session=None):
        if session is None:
            session = requests.Session()
        self.session = session
        #self.geocode_database = Airtable(AIRTABLE_BASE_KEY, AIRTABLE_GEOCODE_TABLE)

    def process_item(self, item, spider):
        """
        Performs geocoding of an event if it doesn't already have
        coordinates.
        """
        if item['location']['coordinates'] is None:
            querydict = self._parse_address(item.get('location', {}), 'Chicago', 'IL')
            print(querydict)
            if not querydict:
                #spider.logger.debug('GEOCODER PIPELINE: Empty query. Not geocoding {0}'.format(item['id']))
                return item
            item['location']['coordinates'] = self._geocode_address(querydict, item, spider)
            item['usaddress_dict'] = querydict
            return item

    def _geocode_address(self, querydict, item, spider):
        '''
        Joins the usaddress parsed components and queries from Tamu 
        Returns a geocoded item with geocode query and results
        '''
        try:
            address = ' '.join(v for (k, v) in querydict.items() if k not in ['PlaceName', 'StateName', 'ZipCode'])
            query = address+' '+querydict['PlaceName']+', '+querydict['StateName']+' '+querydict['ZipCode']
            print('Query: ' + query)
            geocode = geocoder.tamu(address,
                              city=querydict['PlaceName'],
                              state=querydict['StateName'],
                              zipcode=querydict['ZipCode'],
                              session=self.session, key=TAMU_API_KEY)
            print(geocode)
            print(geocode.ok)
            print(geocode.latlng)
        ## start paste
        except ValueError:
            print("Could not Geocode")
            #spider.logger.debug(('GEOCODER PIPELINE: Could not geocode, skipping. '
            #                     'Query: {0}. Item id: {1}').format(query, item['id']))
        except Exception as e:
            print("Unknown error, skip")
            #spider.logger.info(('GEOCODER PIPELINE: Unknown error when geocoding, skipping. '
            #                    'Query: {0}. Item id: {1}. Message: {2}').format(query, item['id'], str(e)))
        else:
            new_data = {
                'location': {
                    'coordinates': {
                        'longitude': str(geocode.latlng[1]),
                        'latitude': str(geocode.latlng[0]),
                    },
                    'name': item.get('location', {'name': ''}).get('name', ''),
                    'address': item.get('location', {'address': ''}).get('address', ''),
                    'url': item.get('location', {'url': ''}).get('url', '')
                },
                'geocode': '',
                'community_area': '',
            }
            geocoded_item = item.copy()
            geocoded_item.update(new_data)
            return geocoded_item
        return {'location': {
                'coordinates': {
                 'latitude': 'None found',
                 'longitude': 'None found',
                },
                'address': ''}}

    def _parse_address(self, location_dict, default_city='Chicago', default_state='IL'):
        """
        Parses address into dictionary of address components using usaddress
        """
        address = location_dict.get('address', '')

        if address is None:
            return {}

        # replace city hall
        address = re.sub('city hall((?!.*chicago, il).)*$',
                         'City Hall 121 N LaSalle St., Chicago, IL',
                         address, flags=re.I)

        try:
            querydict = usaddress.tag(address)[0]
        except usaddress.RepeatedLabelError as ex:
            # @TODO: include multiple errors
            querydict = self.bad_address_tag(ex.parsed_string)

        loc_types = ['PlaceName', 'StateName', 'ZipCode']
        default_locs = [default_city, default_state, '']

        for i in range(3):
            print(i)
            label = loc_types[i]
            loc = default_locs[i]
            # usaddress will return "Southside, Chicago" as city
            #  or "IL, USA" as state
            print(label + ' ' + loc)
            if (label not in querydict) or re.search(',', querydict[label]):
                querydict[label] = loc

        return querydict

    def bad_address_tag(parsed_string):
        """
        Turn the usaddress parse list of tuples with
        RepeatedLabelError issues (duplicate labels)
        Returns the second label as the valid label.
        Thus far the only duplicate label appears to be due to
        incorrectly identifying an earlier string as the PlaceName, Thus
        we are encoding the second PlaceName as the true PlaceName while
        preserving the order in an ordered dictionary.
        """
        counts = collections.Counter(t[1] for t in parsed_string)
        parsedDict = collections.OrderedDict()

        for t in parsed_string:
            num = counts.get(t[1], 0)
            signifier = t[1]
            if num > 1:
                signifier = signifier+str(num)
                counts[t[1]] = num-1
            parsedDict[signifier] = t[0]
        return parsedDict

    def _update_fromDB(self, query, item):
        """
        Query the geocode database and update item
        with results.
        """
        fetched_item = self._geocodeDB_fetch(query)
        try:
            new_data = {
                'location': {
                    'coordinates': {
                        'longitude': str(fetched_item['longitude']),
                        'latitude': str(fetched_item['latitude'])
                    },
                    'name': fetched_item.get('name', ''),
                    'address': fetched_item['address'],
                    'url': item.get('location', {'url': ''}).get('url', '')
                },
                'geocode': str(fetched_item.get('geocode', '')),
                'community_area': fetched_item.get('community_area', '')
            }
        except:
            return {}
        else:
            updated_item = item.copy()
            updated_item.update(new_data)
            return updated_item

    def _geocodeDB_fetch(self, query):
        """
        Fetch from geocode_database.
        """
        try:
            return self.geocode_database.match('mapzen_query', query)['fields']
        except:
            return None

    def _geocodeDB_write(self, spider, item):
        """
        Write to geocode_database.
        """
        #spider.logger.debug('GEOCODER PIPELINE: Caching {0}'.format(item['mapzen_query']))
        item['geocode_date_updated'] = datetime.datetime.now().isoformat()
        airtable_item = self.geocode_database.match('mapzen_query', item['mapzen_query'])
        if airtable_item:
            self.geocode_database.update_by_field('mapzen_query', item['mapzen_query'], item)
        else:
            self.geocode_database.insert(item)
