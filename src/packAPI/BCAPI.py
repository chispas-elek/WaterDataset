# -*- encoding: utf-8 -*-
import urllib2
import urllib
import json
import pprint

__author__ = 'Rubén Mulero'

"""
This file was generated by a sample code in BC's API documentation :

http://docs.ckan.org/en/ckan-2.3/api/index.html

"""


class BCAPI(object):
    def __init__(self):
        # JSON data_string to do a query with the word 'water'
        self.data_string = urllib.quote(json.dumps({'q': 'water',
                                                    #'rows': 10000000
                                                    }))

    def query(self):
        """
        Connects to the British Columbia Catalogue API and returns some data

        :return: A dict of datasets
        """
        result = {}
        try:
            # Make the HTTP request,
            response = urllib2.urlopen('http://catalogue.data.gov.bc.ca/api/1/action/package_search',
                                       self.data_string)
            if response.code == 200:  # Ok
                # Use the json module to load CKAN's response into a dictionary.
                response_dict = json.loads(response.read())
                # Check the contents of the response.
                if response_dict['success']:
                    result = response_dict['result']
                else:
                    raise ErrorResponseDict(pprint.pprint(result))
            else:
                raise ErrorResponseCode(response.code)

        except urllib2.URLError, e:
            print "Problems when connecting to the API. Please check your Internet connection."
            print e

        return result


class ErrorResponseCode(Exception):
    def __init__(self, p_value):
        self.value = p_value

    def __str__(self):
        return "API connection Error. Code: " + str(self.value)

class ErrorResponseDict(Exception):
    def __init__(self, p_value):
        self.value = p_value

    def __str__(self):
        return "API call Error. Check your type of querry and data. More info --> " + str(self.value)
