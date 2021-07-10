from apache_beam.options.pipeline_options import PipelineOptions

BUCKET = "gs://environmental_data/"
FOLDER = "landing_zone/cdmo/advanced_query_system/"
INPUT_PATH = f"{BUCKET}{FOLDER}"
OUTPUT_PATH = f"{BUCKET}/structured_zone/test_output/"


class NERRPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--station_filepattern",
            default=f"{INPUT_PATH}*/*stations*.csv"
        )
        parser.add_argument(
            "--nut_filepattern",
            default=f"{INPUT_PATH}*/*nut*.csv"
        )
        parser.add_argument(
            "--wq_filepattern",
            default=f"{INPUT_PATH}*/*wq*.csv"
        )
        parser.add_argument(
            "--met_filepattern",
            default=f"{INPUT_PATH}*/*met*.csv"
        )
        parser.add_argument(
            "--output",
            default=f"{OUTPUT_PATH}"
        )
