"""
Retrieves tweets given a list of stop-words for a particular period.
For use with Twitter Academic API.

Usage:
sampler_api.py [-h] -bearer_token BEARER_TOKEN 
                    -stop_words_path STOP_WORDS_PATH 
                    -start_year START_YEAR 
                    -stop_year STOP_YEAR
                    [-dir DIR] 
                    [-sleep_duration SLEEP_DURATION]
                    [-retry_duration RETRY_DURATION]

optional arguments:
  -h, --help                          show this help message and exit
  -bearer_token BEARER_TOKEN          Twitter API BEARER_TOKEN.
  -stop_words_path STOP_WORDS_PATH    Path to the stopwords csv or tsv file.
  -start_year START_YEAR              Year to start tweet collection.
  -stop_year STOP_YEAR                Year to end tweet collection.
  -dir DIR                            Directory for storing responses.
  -sleep_duration SLEEP_DURATION      How many seconds to wait between requests.
  -retry_duration RETRY_DURATION      How many seconds to wait after failed request.

Example:
$ python scripts/sampler_api.py -bearer_token XXXXX -stop_words_path hausa-stopwords.csv -start_year 2022 -stop_year 2022
"""

import argparse
import os
import sys
import json
import time
from typing_extensions import Required
import requests
import logging
import pandas as pd

from os import listdir

from utils import get_periods, validate_stopwords_file, validate_lang_code, validate_place, validate_time_range, validate_point_radius, validate_coords

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')

def query_twitter(search_url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    if response.status_code != 200:
      raise Exception(response.status_code, response.text)
    return response.json()

def build_query(stop_words, lang, place, country, point_radius, coords, query_options):
  sep = ' '
  if stop_words:
    stop_word_string = '('
    end_string = ')' + sep
  else:
    stop_word_string = ''
    end_string = sep

  if lang:
    end_string += lang + sep
  if place:
    end_string += place + sep
  if country:
    end_string += country + sep
  if point_radius:
    end_string += point_radius + sep
  if coords:
    end_string += coords + sep

  for n, word in enumerate(stop_words):
    stop_word_string += f'"{word}"'
    n += 1
    if n < len(stop_words):
      stop_word_string += ' OR '

  stop_word_string += end_string
  stop_word_string += query_options

  print('Query string: %s' % stop_word_string)
  print('Query length: %s' % len(stop_word_string))
  
  return stop_word_string

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Retrieves generic tweets for every hour of every day of the given YYYY-MM at the specified MIN_MARK.')
    parser.add_argument('-bearer_token', help='Twitter API BEARER_TOKEN.', required=True, type=str)
    parser.add_argument('-stop_words_path', help='Path to the stopwords csv or tsv file.', type=validate_stopwords_file)
    parser.add_argument('-stopwords_column_name', help='Stopwords column name.', default='', type=str)
    parser.add_argument('-has_header', help='Specify if the lexicon file has a header row.', action='store_true')
    parser.add_argument('-time_range', choices=['hourly', 'daily', 'weekly', 'monthly', 'yearly'], help='Time range: hourly, daily, weekly, monthly or yearly.', default='hourly', type=validate_time_range)
    parser.add_argument('-lang', help='Language code.', default=None, type=validate_lang_code)
    parser.add_argument('-without_stopwords', help='Specify if crawling without stopwords.', action='store_true')
    parser.add_argument('-query_options', help='Query options, e.g. has:media for tweets that contain any media.', required=True, type=str)
    parser.add_argument('-location_type', choices=['place', 'place_country', 'point_radius', 'bounding_box'], default=None, help='the retrieval method (nn: standard nearest neighbor; invnn: inverted nearest neighbor; invsoftmax: inverted softmax; csls: cross-domain similarity local scaling)')
    parser.add_argument('-place', default=None, type=str, help='provide place name if place option is selected.')
    parser.add_argument('-country', default=None, type=validate_place, help='provide country if place_country option is selected.')
    parser.add_argument('-point_radius', default=None, type=str, help='provide "point,radius" if point_radius option is selected.')
    parser.add_argument('-coords', default=None, type=str, help='provide the geographic coordinates if bounding_box options is selected.')
    parser.add_argument('-start_year', help='Year to start tweets collection from.', required=True, type=int)
    parser.add_argument('-stop_year', help='Year to stop tweets collection.', required=True, type=int)
    parser.add_argument('-dir', help='Directory for storing responses.', default='data/responses/', required=False, type=str)
    parser.add_argument('-sleep_duration', help='How many seconds to wait between requests.', default=5, required=False, type=int)
    parser.add_argument('-retry_duration', help='How many seconds to wait after failed request.', default=61, required=False, type=int)
    args = parser.parse_args()

    assert args.start_year >= 2006
    assert args.stop_year >= args.start_year
    #assert args.hour_mark >= 0 and args.hour_mark < 24

    if not os.path.exists(args.dir):
      os.makedirs(args.dir)

    bearer_token = args.bearer_token
    stop_words_path = args.stop_words_path
    column_name = args.stopwords_column_name
    lt = args.location_type

    if args.lang:
      lang = 'lang:' + args.lang
    else:
      lang = None
    
    if lt == 'place':
      assert args.place is not None, 'place option was selected, provide place name'
      place = 'place:' + args.place
      country = None
      point_radius = None
      coords = None
    elif lt == 'place_country':
      assert args.country is not None, 'place_country option was selected, provide country code'
      country = 'place_country:' + args.country
      place = None
      point_radius = None
      coords = None
    elif lt == 'point_radius':
      assert args.point_radius is not None, 'point_radius was selected but no point and radius is provided in the format "long. lat. radius" or "long.,lat.,radius"'
      point_radius = validate_point_radius(args.point_radius)
      place = None
      country = None
      coords = None
    elif lt == 'bounding_box':
      assert args.coords is not None, 'bounding_box was selected but no coordinates are provided in the format "west_long south_lat east_long north_lat" or "west_long,south_lat,east_long,north_lat"'
      coords = validate_coords(args.coords)
      place = None
      country = None
      point_radius = None
    else:
      place = None
      country = None
      point_radius = None
      coords = None

    if args.without_stopwords:
      stop_words = []
      query_string = build_query(stop_words, lang, place, country, point_radius, coords, args.query_options)
    else:
      header = None
      if args.has_header:  
        header = 0

      if stop_words_path.endswith('.csv'):
        stop_words = pd.read_csv(stop_words_path, header=header)
      else:
        stop_words = pd.read_csv(stop_words_path, header=header, sep='\t')

      if not column_name == '':
        try:
          stop_words = stop_words[column_name]
          #print('Lexicons', stop_words)
        except:
          print('Invalid column name. Please provide the correct entry.')
          sys.exit(-1)
      else:
        stop_words = list(stop_words[0])

      query_string = build_query(stop_words, lang, place, country, point_radius, coords, args.query_options)

    search_url = "https://api.twitter.com/2/tweets/search/all"
    query_params = {}
    query_params['query'] = query_string
    query_params['expansions'] = 'author_id,geo.place_id,attachments.media_keys'
    query_params['tweet.fields'] = 'id,text,created_at,geo,public_metrics,possibly_sensitive'
    query_params['place.fields'] = 'id,full_name,name,country,geo'
    query_params['user.fields'] = 'location'
    query_params['media.fields'] = 'duration_ms,height,media_key,preview_image_url,public_metrics,type,url,width,alt_text'
    query_params['max_results'] = 500

    print(query_params)

    headers = {}
    headers['Authorization'] = "Bearer {}".format(bearer_token)
    
    if len(query_string) > 1024:
      
      print('Query length exceeds 1024 characters, the limit for Academic API. Reduce the number of stopwords.')

    else:
      
      all_periods = get_periods(args.time_range, args.start_year, args.stop_year)
      
      # check responses already collected
      responses_collected = set()
      for fn in listdir(args.dir):
        if fn.endswith('.response.json'):
          responses_collected.add(fn)

      n_requests_until_fail = 0
      for day_periods in all_periods:

        for start_time, end_time in day_periods:

          response_fn = '%s_%s.response.json' % (start_time.replace(':', ''), end_time.replace(':', ''))

          if response_fn in responses_collected:
            print('Found %s ...' % response_fn)
            continue

          while True:

            try:
              logging.info('Requesting %s - %s ...' % (start_time, end_time))
              query_params['start_time'] = start_time
              query_params['end_time'] = end_time
              twitter_response = query_twitter(search_url, headers, query_params)
              wrapped_response = {'start_time': start_time, 'end_time': end_time, 'response': twitter_response}

              logging.info('\tResults Count: %d' % twitter_response['meta']['result_count'])

              logging.info('\tWriting %s ...' % response_fn)
              with open(os.path.join(args.dir, response_fn), 'w') as jl_f:
                  json.dump(wrapped_response, jl_f, indent=4)

              logging.info('\tSleeping %f secs ...' % args.sleep_duration)
              time.sleep(args.sleep_duration)
              n_requests_until_fail += 1
              break

            except Exception as e:
              logging.info('\tRequest Failed - ', e)
              logging.info('\t# requests until fail:', n_requests_until_fail)
              logging.info('\tSleeping %d secs ...' % args.retry_duration)
              time.sleep(args.retry_duration)
              n_requests_until_fail = 0
              args.sleep_duration += 0.01