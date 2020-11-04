#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import csv
import logging
import sys

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import trigger

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="dataengineering-course-5fc3ec3747be.json"

import re

import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
sentiment_analyzer = SentimentIntensityAnalyzer()

class ParseRows(beam.DoFn):
    """Parses the raw tweet info into a Python dictionary.
    Each event line has the following format:
      user_id, tweet, timestamp
    e.g.:
      @oledi45, "@BarackObama is going on the campaign trail, so now it’s time to ask him about the Russian collusion hoax he led, and if he knew @JoeBiden
 was getting money from Hunter Biden selling access.", 2020-10-22 10:00:04
    """

    def __init__(self):
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem): #it is a Beam.DoFn so it has a process function
        try:
            row = list(csv.reader([elem]))[0]
            yield {
                'text': row[0],
                'user_id': row[17],
                'timestamp': row[1],
            }
        except:
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)

def preprocess(tweet):
    """This receives the tweet text and cleans it by removing URLs, numbers, symbols, punctuation, and lowercases the text
    The clean tweet is returned"""
    split_tweet = tweet.split()
    # remove tags and URLs
    split_tweet = [word for word in split_tweet if 'http' not in word and 'www' not in word]
    tweet = ' '.join(split_tweet)
    regex = re.compile('[^A-Za-z ]')
    clean_tweet = regex.sub('', tweet.strip().lower())
    return clean_tweet


def prediction(tweet):
    """This receives the clean tweet text and predicts its sentiment using the nltk Sentiment Intensity Analyzer (vader lexicon)
    The analyzer will generate a dictionary with the scores for positive, neutral, negative and compound sentiment
    We return the compound score and a label based on this"""
    result = sentiment_analyzer.polarity_scores(tweet)
    score = [value for key, value in result.items() if key == 'compound'][0]
    if score >= 0.05:
        label = 'pos'
    elif score <= -0.05:
        label = 'neg'
    else:
        label = 'neu'

    return (label, score)

class MyPredictDoFn(beam.PTransform):
    """This is the pipeline part that makes sure the sentiment is analyzed and saved by calling the necessary functions"""
    def __init__(self):
        beam.PTransform.__init__(self)

    def expand(self, pcoll, **kwargs): #it is a Beam.PTransform so it has an expand function
        """For each row in the data, return user_id, clean tweet, timestamp, and the sentiment (label, score)"""
        return (
                pcoll
                | 'preprocess' >> beam.Map(lambda elem: (elem['user_id'], preprocess(elem['text']), elem['timestamp'], prediction(preprocess(elem['text'])))))


class SentimentDict(beam.DoFn):
    """Formats the data into a dictionary of BigQuery columns with their values
    Receives a (user_id, clean_tweet, timestamp, sentiment(label + score)) combination,
    creates an extra column based on the presidential candidate mentioned,
    and formats everything together into a dictionary. The dictionary is in the format
    {'bigquery_column': value}
    """

    def process(self, all_info, window=beam.DoFn.WindowParam): #it is a Beam.DoFn so it has a process function
        print('This is the result', all_info)
        user_id, tweet, timestamp, sentiment = all_info
        if 'biden' in tweet and 'trump' in tweet:
            candidate = 'Both'
        elif 'biden' in tweet:
            candidate = 'Biden'
        elif 'trump' in tweet:
            candidate = 'Trump'
        else:
            candidate = None
        yield {
            'user_id': user_id,
            'tweet': tweet,
            'sentiment_label': sentiment[0],
            'compound_score': sentiment[1],
            'time_stamp': timestamp,
            'candidate': candidate
        }


class WriteToBigQuery(beam.PTransform):
    """Generate, format, and write BigQuery table row information."""

    def __init__(self, table_name, dataset, schema, project):
        """Initializes the transform.
        Args:
          table_name: Name of the BigQuery table to use.
          dataset: Name of the dataset to use.
          schema: Dictionary in the format {'column_name': 'bigquery_type'}
          project: Name of the Cloud project containing BigQuery table.
        """
        beam.PTransform.__init__(self)
        self.table_name = table_name
        self.dataset = dataset
        self.schema = schema
        self.project = project

    def get_schema(self):
        """Build the output table schema."""
        return ', '.join('%s:%s' % (col, self.schema[col]) for col in self.schema)

    def expand(self, pcoll): #it is a Beam.PTransform so it has an expand function
        return (
                pcoll
                | 'ConvertToRow' >>
                beam.Map(lambda elem: {col: elem[col]
                                       for col in self.schema})
                | beam.io.WriteToBigQuery(
            self.table_name, self.dataset, self.project, self.get_schema()))

class CalculateSentimentScores(beam.PTransform):
    """Calculates sentiment for each tweet within the configured window duration.
    Extract necessary info from the event stream, using minute-long windows.
    """

    def __init__(self, window_duration, allowed_lateness):
        beam.PTransform.__init__(self)
        self.window_duration = window_duration * 60
        self.allowed_lateness_seconds = allowed_lateness * 60

    def expand(self, pcoll): #it is a Beam.PTransform so it has an expand function
        return (
                pcoll
                # We will get early (speculative) results as well as cumulative
                # processing of late data.
                | 'TweetFixedWindows' >> beam.WindowInto(
            beam.window.FixedWindows(self.window_duration),
            trigger=trigger.AfterWatermark(
                trigger.AfterCount(10), trigger.AfterCount(20)),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=self.allowed_lateness_seconds)
                | 'Predict' >> MyPredictDoFn())

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the sentiment pipeline."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--topic', type=str, help='Pub/Sub topic to read from')
    parser.add_argument(
        '--subscription', type=str, help='Pub/Sub subscription to read from')
    parser.add_argument(
        '--dataset',
        type=str,
        required=True,
        help='BigQuery Dataset to write tables to. '
             'Must already exist.')
    parser.add_argument(
        '--table_name',
        default='leader_board',
        help='The BigQuery table name. Should not already exist.')
    parser.add_argument(
        '--window_duration',
        type=int,
        default=1,
        help='Numeric value of fixed window duration for sentiment '
             'analysis, in minutes')
    parser.add_argument(
        '--allowed_lateness',
        type=int,
        default=2,
        help='Numeric value of allowed data lateness, in minutes')

    known_args, pipeline_args = parser.parse_known_args(argv)


    if known_args.topic is None and known_args.subscription is None:
        parser.print_usage()
        print(sys.argv[0] + ': error: one of --topic or --subscription is required')
        sys.exit(1)

    options = PipelineOptions(pipeline_args)

    # We also require the --project option to access --dataset
    if options.view_as(GoogleCloudOptions).project is None:
        parser.print_usage()
        print(sys.argv[0] + ': error: argument --project is required')
        sys.exit(1)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    options.view_as(SetupOptions).save_main_session = save_main_session

    # Enforce that this pipeline is always run in streaming mode
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        # Read tweet information from Pub/Sub and parse the data.

        # Read from PubSub into a PCollection.
        if known_args.subscription:
            scores = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(
                subscription=known_args.subscription)
        else:
            scores = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(topic=known_args.topic)

        events = (
                scores
                | 'DecodeString' >> beam.Map(lambda b: b.decode('utf-8'))
                | 'ParseRows' >> beam.ParDo(ParseRows()))


        # Get tweet sentiments and write the results to BigQuery
        (
                events
                | 'CalculateSentimentScores' >> CalculateSentimentScores(
            known_args.window_duration, known_args.allowed_lateness)
                | 'SentimentDict' >> beam.ParDo(SentimentDict())
                | 'WriteResults' >> WriteToBigQuery(
            known_args.table_name,
            known_args.dataset,
            {
                'user_id': 'STRING',
                'tweet': 'STRING',
                'sentiment_label': 'STRING',
                'compound_score': 'NUMERIC',
                'time_stamp': 'STRING',
                'candidate': 'STRING',
            },
            options.view_as(GoogleCloudOptions).project))



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()