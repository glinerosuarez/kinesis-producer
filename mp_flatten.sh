#!/bin/bash

# specify the month (in format YYYY-MM)
reading_type=$1
year=$2

for month in {10..12}; do
  year_month="$year-$month"
  # get the number of days in the month
  days_in_month=$(date -d "$year_month-01 + 1 month - 1 day" +%d)
  # loop through the days
  for day in $(seq 1 $days_in_month); do
      # format the day to have leading zeroes
      formatted_day=$(printf "%02d" $day)
      # construct the date in format YYYY-MM-DD
      date="$year_month-$formatted_day"
      # run the command
      python3.7 file_flattener.py $reading_type $year $month $formatted_day
  done
done