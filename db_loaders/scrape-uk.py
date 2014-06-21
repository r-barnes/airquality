# Scrape UK air quality data

import urllib
import urllib2
import webbrowser
import json
import datetime

def retrieve_data(monitoring_station, **options):
    url = "http://uk-air.defra.gov.uk/data/data_selector"        
    params = {
        'q': '515237',
        'f_statistic_type_id': '9999',
        'action': 'step6',
        'f_limit_was': '1',
        'submit': 'Save selection',
        'f_preset_date': '1',
        'f_site_id[]': monitoring_station,
    }
    if options:
        params = dict(params.items() + options.items())

    data = urllib.urlencode(params)
    results = urllib2.urlopen(url, data)
    
    with open("results.html", "w") as f:
        f.write(results.read())
    webbrowser.open("results.html")

retrieve_data('ABD')


