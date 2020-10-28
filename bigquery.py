import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import trigger
import argparse
from csv import reader
from datetime import datetime
import logging
import os
import sys
from time import time

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/s161158/Downloads/dataengineering-course-a9365e622cc2.json"

class ParseTweetFn(beam.DoFn):
    # hybrid Ellen/lab
    """
    Parses the raw tweet data into a Python dictionary

    Each line has the following format (e.g.):
        Content ([tweet text]), Date (22-10-2020  9:54:51), Language (en),
        Location (US), Number of Likes (1), Number of Retweets (0),
        In Reply To ([tag]), Author Name ([profile name]), Author Description ([profile bio]),
        Author Statuses Count (29201), Author Favourites Count (91873), Author Friends Count (21468),
        Author Followers Count (22021), Author Listed Count (13), Author Verified (FALSE),
        Longitude (blank), Latitude (blank), Author ([tag])
    """

    def __init__(self):
        # copied from lab code
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        # copied from ellen's code (27-10)
        try:
            row = list(reader([elem]))[0]
            yield {
                'text': row[0],
                'user_id': row[17],
                'score': row[4],  # number of likes, to bridge the gap between the lab example and our project
                'timestamp': row[1]
            }
        except:
            self.num_parse_errors.inc()
            logging.error(f'Parse error on {elem}')


class ExtractAndSumLikes(beam.PTransform):
    # copied from lab
    """
    A transform to extract the user/likes information and sum the likes.
    The constructor argument 'field' determines whether 'team' or 'user' info is extracted.
    """

    def __init__(self, field):
        # copied from lab
        beam.PTransform.__init__(self)
        self.field = field

    def expand(self, pcoll):
        # copied from lab
        return (pcoll
                | beam.Map(lambda elem: (elem[self.field], elem['score']))
                | beam.CombinePerKey(sum))


def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
    # copied from lab
    """
    Converts a unix timestamp into a formatted string.
    This is needed in the TeamScoresDict class, to format the window start/stop timestamps.
    """
    return datetime.fromtimestamp(t).strftime(fmt)


class TeamScoresDict(beam.DoFn):
    """"
    Formats the data into a dictionary of BigQuery columns with their values

    Received a (user, score) pair, extracts the window start timestamp,
    and formats everything together into a dictionary.

    This dictionary is in the format {'bigquery_column': value}
    """

    def process(self, user_score, window=beam.DoFn.WindowParam):
        user, score = user_score
        start = timestamp2str(int(window.start))
        yield {
            'user': user,
            'total_likes': score,
            'window_start': start,
            'processing_time': timestamp2str(int(time()))
        }


class WriteToBigQuery(beam.PTransform):
    # copied from lab
    """
    Generate, format, and write BigQuery table row information.
    """

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
        """
        Build the output table schema
        """
        return ', '.join('%s:%s' % (col, self.schema[col]) for col in self.schema)

    def expand(self, pcoll):
        return (pcoll
                | 'ConvertToRow' >> beam.Map(lambda elem: {col: elem[col] for col in self.schema})
                | beam.io.WriteToBigQuery(self.table_name, self.dataset, self.project, self.get_schema()))


# class CalculateTeamScores is not necessary to implement

class CalculateUserLikes(beam.PTransform):
    # copied from lab
    """
    Extract user/like pairs from the event stream using processig time, via global windowing.
    Get periodic updates on al users' running 'scores'.
    """

    def __init__(self, allowed_lateness):
        beam.PTransform.__init__(self)
        self.allowed_lateness_seconds = allowed_lateness * 60

    def expand(self, pcoll):
        return (pcoll
                | 'LeaderboardUsersGlobalWindows' >> beam.WindowInto(beam.window.GlobalWindows(), trigger=trigger.Repeatedly(trigger.AfterCount(10)),
                                                                     accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                                                                     allowed_lateness=self.allowed_lateness_seconds)
                | 'ExtractAndSumScore' >> ExtractAndSumLikes('user_id'))


def run(argv=None, save_main_session=True):
    """
    Main entry point; defines and runs the hourly_user_likes pipeline
    """

    # first we set up the argument parser
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--topic',
        type=str,
        default='projects/dataengineering-course/topics/usdata',
        help='Pub/Sub topic to read from')
    parser.add_argument(
        '--subscription',
        type=str,
        help='Pub/Sub subscription to read from')
    parser.add_argument(
        '--dataset',
        type=str,
        default='usdata',
        required=True,
        help='BigQuery Dataset to write tables to. Must already exist.')
    parser.add_argument(
        '--table_name',
        default='testellen',
        help='The BigQuery table name. Should not already exist.')
    parser.add_argument(
        '--team_window_duration',
        type=int,
        default=3,
        help='Numeric value of fixed window duration for team analysis, in minutes')
    parser.add_argument(
        '--allowed_lateness',
        type=int,
        default=6,
        help='Numeric value of allowed data lateness, in minutes')

    args, pipeline_args = parser.parse_known_args(argv)

    if args.topic is None and args.subscription is None:
        parser.print_usage()
        print(sys.argv[0] + 'error: one of --topic or -- subscription is required')
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
        # Read game events from Pub/Sub using custom timestamps, which are extracted from the pubsub data elements, and parse the data.

        # Read from Pub/Sub into a PCollection
        if args.subscription:
            scores = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(subscription=args.subscription)
        else:
            scores = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(topic=args.topic)

        events = (
                scores
                | 'Decodestring' >> beam.Map(lambda b: b.decode('utf-8'))
                | 'ParseTweetFn' >> beam.ParDo(ParseTweetFn())
                # possible error, we have no timestamp as input
                | 'AddEventTimestamps' >> beam.Map(lambda elem: beam.window.TimestampedValue(elem, 0)))# MAADD HACKZZX elem['timestamp'])))

        # Get user scores and write the results to BigQuery
        (events
         | 'CalculateTeamScores' >> CalculateUserLikes(args.allowed_lateness)
         | 'TeamScoresDict' >> beam.ParDo(TeamScoresDict())
         | 'WriteTeamScoreSums' >> WriteToBigQuery(args.table_name + '_test',
                                                   args.dataset,
                                                   {'user': 'STRING',
                                                    'total_likes': 'INTEGER',
                                                    'window_start': 'STRING',
                                                    'processing_time': 'STRING', },
                                                   options.view_as(GoogleCloudOptions).project))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()