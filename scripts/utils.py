import re
import sys

import pandas as pd
import pycountry

from datetime import datetime, timedelta
from langcodes import Language
from argparse import ArgumentTypeError

def validate_stopwords_file(astring):
  if not (astring.endswith('.tsv') or astring.endswith('.csv')):
    raise ArgumentTypeError("%s: is an invalid file, provide a csv or tsv." % astring)
  return astring

def validate_time_range(astring):
  if astring not in ['hourly','daily','weekly','monthly','yearly']:
    raise ArgumentTypeError("%s: is an invalid time range, provide either hourly, daily, weekly, monthly or yearly." % astring)
  return astring

def validate_lang_code(astring):
  supported_langs = ['am', 'de', 'ml', 'sk', 'ar', 'el', 'dv', 'sl', 'hy', 'gu', 'mr', 'ckb', 'eu', 'ht', 'ne', 'es', 'bn', 
                    'iw', 'no', 'sv', 'bs', 'hi', 'or', 'tl', 'bg', 'hi-Latn', 'pa', 'ta', 'my', 'hu', 'ps', 'te', 'hr', 'is', 
                    'fa', 'th', 'ca', 'in', 'pl', 'bo', 'cs', 'it', 'pt', 'zh-TW', 'da', 'ja', 'ro', 'tr', 'nl', 'kn', 'ru', 'uk', 
                    'en', 'km', 'sr', 'ur', 'et', 'ko', 'zh-CN', 'ug', 'fi', 'lo', 'sd', 'vi', 'fr', 'lv', 'si', 'cy', 'ka', 'lt']
  if not Language.get(astring.lower()).is_valid():
    raise ArgumentTypeError("%s: is an invalid language code." % astring)
  if not astring.lower() in supported_langs:
    print("%s: is either not supported by Twitter or check the correct code from Twitter documentation." % astring)
    astring = False
  return astring

def validate_place(astring):
  if not pycountry.countries.get(alpha_2=astring):
    raise ArgumentTypeError("%s: is an invalid country code." % astring)
  return astring

def validate_point_radius(astring):
  inp_loc = [l for l in re.findall(r'\s|,|[^,\s]+', astring) if l not in [',', ' ']]
  if len(inp_loc) == 2:
    lon, rad = inp_loc
    assert lon >= -180 and lon <= 180, 'Invalid longitude range.'
    return 'point_radius:[' + str(lon) + ' ' + str(rad) + ']'
  if len(inp_loc) == 3:
    lon, lat, rad = inp_loc
    assert lon >= -180 and lon <= 180, 'Invalid longitude range.'
    assert lat >= -90 and lat <= 90, 'Invalid latitude range.'
    return 'point_radius:[' + str(lon) + ' ' + str(lat) + ' ' + str(rad) + ']'
  else:
    print('Coordinates wrong format. Provide as "long. lat. radius" or "long.,lat.,radius".')
    sys.exit(-1)

def validate_coords(astring):
  inp_loc = [l for l in re.findall(r'\s|,|[^,\s]+', astring) if l not in [',', ' ']]
  if len(inp_loc) == 4:
    w_lon, s_lat, e_lon, n_lat = inp_loc
    assert w_lon >= -180 and w_lon <= 180 and e_lon >= -180 and e_lon <= 180, 'Invalid longitude range.'
    assert s_lat >= -90 and s_lat <= 90 and n_lat >= -90 and n_lat <= 90, 'Invalid latitude range.'
    return 'bounding_box:[' + str(w_lon) + ' ' + str(s_lat) + ' ' + str(e_lon) + ' ' + str(n_lat) + ']'
  else:
    print('Coordinates wrong format. Provide as "west_long south_lat east_long north_lat" or "west_long,south_lat,east_long,north_lat".')
    sys.exit(-1)

def get_number_of_days(start_year, stop_year):
  total_days = 0
  for year in range(start_year, stop_year + 1):
    total_days += sum([pd.Period(f'{year}-{i}-1').daysinmonth for i in range(1,13)])

  return total_days

def check_invalid_date(year, month, day, hour):
  try:
    dt = datetime(year, month, day, hour)
  except ValueError:
    return True

  dt_now = datetime.now()
  dt_diff = dt_now.timestamp() - dt.timestamp()
  if dt_diff < (90 * 60):  # min 1h30m diff
    return True
  
  return False

def get_periods(time_range, start_year, stop_year):

  all_periods = []
  day_periods = []
  stop = False

  if time_range == 'hourly':
    for year in range(start_year, stop_year + 1):  
      for month in range(1, 12+1):
        for day in range(1, 31+1):
          for hour in range(0, 23+1):
            if check_invalid_date(year, month, day, hour):
              continue

            start_time = datetime(year, month, day, hour)
            end_time = start_time + timedelta(hours=1)

            start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

            day_periods.append((start_time, end_time))
  
  if time_range == 'daily':
    for year in range(start_year, stop_year + 1):  
      for month in range(1, 12+1):
        for day in range(1, 31+1):
          if check_invalid_date(year, month, day, 0):
            continue

          start_time = datetime(year, month, day)
          end_time = start_time + timedelta(days=1)
          date_today = datetime.now()

          if (date_today - end_time).total_seconds() < 0:
            end_time = date_today - timedelta(hours=1)
            stop = True

          start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
          end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

          day_periods.append((start_time, end_time))

          if stop:
            break

  if time_range == 'weekly':
    start_time = datetime(start_year, 1, 1, 0)
    for day in range(1, get_number_of_days(start_year, stop_year), 7):
      if check_invalid_date(start_time.year, start_time.month, start_time.day, 0):
        continue

      end_time = start_time + timedelta(days=7)
      date_today = datetime.now()

      if (date_today - end_time).total_seconds() < 0:
        end_time = date_today
        stop = True

      str_start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
      str_end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

      day_periods.append((str_start_time, str_end_time))

      if stop:
        break

      start_time = end_time

  if time_range == 'monthly':
    for year in range(start_year, stop_year + 1):  
      for month in range(1, 12+1):
        if check_invalid_date(year, month, 1, 0):
          continue

        start_time = datetime(year, month, 1, 0)
        
        next_month = month + 1
        next_year = year
        if next_month > 12:
          next_year += 1
          next_month = 1
        
        end_time = datetime(next_year, next_month, 1, 0) - timedelta(seconds=1)
        date_today = datetime.now()

        if (date_today - end_time).total_seconds() < 0:
          end_time = date_today
          stop = True

        start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        day_periods.append((start_time, end_time))

        if stop:
          break

  if time_range == 'yearly':
    for year in range(start_year, stop_year + 1):  
      if check_invalid_date(year, 1, 1, 0):
        continue

      start_time = datetime(year, 1, 1, 0)
      end_time = datetime(year + 1, 1, 1, 0) - timedelta(seconds=1)

      date_today = datetime.now()

      if (date_today - end_time).total_seconds() < 0:
        end_time = date_today
        stop = True

      start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
      end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

      day_periods.append((start_time, end_time))

      if stop:
        break
    
  if len(day_periods) > 0:
    all_periods.append(day_periods)

  return all_periods