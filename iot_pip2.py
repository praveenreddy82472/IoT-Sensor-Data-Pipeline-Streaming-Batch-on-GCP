import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Timestamp
from datetime import datetime
import logging


# ---------- Helper Functions ----------
def parse_csv(line):
    fields = line.strip().split(',')
    if fields[0] == 'sensor_id':
        return None
    try:
        parsed_time = datetime.strptime(fields[1], '%Y-%m-%d %H:%M:%S')
        # Make datetime timezone-aware (UTC)
        parsed_time = parsed_time.replace(tzinfo=datetime.timezone.utc)
        beam_ts = Timestamp.from_utc_datetime(parsed_time)
        return {
            'sensor_id': fields[0],
            'timestamp': beam_ts.to_rfc3339(),
            'temperature': float(fields[2]),
            'humidity': float(fields[3]),
            'location': fields[4],
            'ingestion_type': 'batch'
        }
    except Exception as ex:
        logging.warning(f"[CSV PARSE ERROR] {line} - {str(ex)}")
        return None

def parse_pubsub(message):
    try:
        record = json.loads(message.decode('utf-8'))
        logging.info(f"[DEBUG-PUBSUB] Raw Message: {record}")
        if not all(k in record for k in ['sensor_id', 'timestamp', 'temperature', 'humidity', 'location']):
            logging.warning(f"[SKIP] Missing fields: {record}")
            return None

        try:
            ts = datetime.fromisoformat(record['timestamp'])
        except:
            ts = datetime.strptime(record['timestamp'], '%Y-%m-%d %H:%M:%S')

        return {
            'sensor_id': record['sensor_id'],
            'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S'),
            'temperature': float(record['temperature']),
            'humidity': float(record['humidity']),
            'location': record['location'],
            'ingestion_type': 'stream'
        }
    except Exception as e:
        logging.error(f"[ERROR-PARSING] {e}")
        return None

class AvgCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return (0.0, 0)
    def add_input(self, acc, input):
        return acc[0] + input, acc[1] + 1
    def merge_accumulators(self, accs):
        sums, counts = zip(*accs)
        return sum(sums), sum(counts)
    def extract_output(self, acc):
        return acc[0] / acc[1] if acc[1] != 0 else float('NaN')

def format_agg(sensor_id, agg_dict, window):
    return {
        'sensor_id': sensor_id,
        'window_start': window.start.to_utc_datetime().strftime('%Y-%m-%d %H:%M:%S'),
        'window_end': window.end.to_utc_datetime().strftime('%Y-%m-%d %H:%M:%S'),
        'avg_temperature': agg_dict['avg_temperature'],
        'avg_humidity': agg_dict['avg_humidity']
    }

# ---------- Pipeline ----------
def run():
    class CustomOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument('--input', type=str, help='GCS batch file path')
            parser.add_argument('--inputSubscription', type=str, help='Pub/Sub subscription')
            parser.add_argument('--output_raw', type=str, help='Raw BQ table')
            parser.add_argument('--output_agg', type=str, help='Agg BQ table')

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'iot-sensor-462716'
    google_cloud_options.region = 'us-west1'
    google_cloud_options.temp_location = 'gs://iot-sensor18/temp'
    google_cloud_options.staging_location = 'gs://iot-sensor18/staging'
    google_cloud_options.template_location = 'gs://iot-sensor18/templates/iot_template'
    google_cloud_options.job_name = 'iot-classic-pipeline'

    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(StandardOptions).streaming = True

    custom_options = options.view_as(CustomOptions)

    with beam.Pipeline(options=options) as p:
        input_path = custom_options.input
        if hasattr(input_path, 'get'):
            input_path = input_path.get()
        # Batch (GCS)
        gcs_data = (
            p
            | 'Read GCS CSV' >> ReadFromText(input_path)
            | 'Parse GCS' >> beam.Map(parse_csv)
            | 'Log GCS Records' >> beam.Map(lambda x: logging.info(f"[GCS RECORD] {x}") or x)
            | 'Filter GCS None' >> beam.Filter(lambda x: x is not None)
        )

        # Stream (Pub/Sub)
        pubsub_data = (
            p
            | 'Read PubSub' >> ReadFromPubSub(subscription=custom_options.inputSubscription)
            | 'Parse PubSub' >> beam.Map(parse_pubsub)
            | 'Log PubSub Records' >> beam.Map(lambda x: logging.info(f"[PUBSUB RECORD] {x}") or x)
            | 'Filter PubSub None' >> beam.Filter(lambda x: x is not None)
        )

        # Merge both
        unified = (gcs_data, pubsub_data) | 'Merge Sources' >> beam.Flatten()

        # Write raw data to BigQuery
        unified | 'Write Raw to BQ' >> WriteToBigQuery(
            table=custom_options.output_raw,
            schema='sensor_id:STRING,timestamp:TIMESTAMP,temperature:FLOAT,humidity:FLOAT,location:STRING,ingestion_type:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Aggregation
        windowed = unified | 'Apply Window' >> beam.WindowInto(FixedWindows(60))

        avg_temp = (
            windowed
            | 'Map Temp' >> beam.Map(lambda x: (x['sensor_id'], x['temperature']))
            | 'Combine Temp' >> beam.CombinePerKey(AvgCombineFn())
        )

        avg_hum = (
            windowed
            | 'Map Humidity' >> beam.Map(lambda x: (x['sensor_id'], x['humidity']))
            | 'Combine Humidity' >> beam.CombinePerKey(AvgCombineFn())
        )

        aggregated = (
            {'avg_temperature': avg_temp, 'avg_humidity': avg_hum}
            | 'Join Metrics' >> beam.CoGroupByKey()
            | 'Format Output' >> beam.Map(lambda kv: (
                kv[0],
                {
                    'avg_temperature': kv[1]['avg_temperature'][0] if kv[1]['avg_temperature'] else float('NaN'),
                    'avg_humidity': kv[1]['avg_humidity'][0] if kv[1]['avg_humidity'] else float('NaN')
                }
            ))
            | 'Add Window Info' >> beam.MapTuple(
                lambda sensor_id, agg_dict, window=beam.DoFn.WindowParam: format_agg(sensor_id, agg_dict, window)
            )
        )

        # Write aggregated data to BigQuery
        aggregated | 'Write Agg to BQ' >> WriteToBigQuery(
            table=custom_options.output_agg,
            schema='sensor_id:STRING,window_start:TIMESTAMP,window_end:TIMESTAMP,avg_temperature:FLOAT,avg_humidity:FLOAT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    run()
