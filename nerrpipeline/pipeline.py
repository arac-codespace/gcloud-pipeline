from database import EnvironmentalDataConnection
import sys

import apache_beam as beam
from apache_beam.options import pipeline_options

import datasource
from options import NERRPipelineOptions
from database import DatabaseConnection
from postgresql_sink import WriteToPostgres


def parse_rows(dataparser, row):
    """
        Parses the rows using the appropiate data parser
        handles the different exceptions that may be raised
        by the code.

        May want to implement a way to persist malformed rows,
        notify and allow the rest of the code/data to be processed.
    """
    if not row:
        return None
    else:
        try:
            results = dataparser.parse_row(row)
        except TypeError as err:
            """
                An unexpected value was passed to the parsing functions.
                May be the result of the dtype conversion methods trying
                to convert an invalid dtype.

                Possible culprits:
                    dateutil.parser.parse(non_str_or_date)
                    int or float(non_numeric)
            """
            print(err)
            raise
        except AttributeError as err:
            """
                A method was attempted to be exectued by an object that doesn't
                have such method.

                Possible culprits:
                    non_str.strip()
            """
            print(err)
            raise
        except Exception as err:
            """
                Something went very wrong...
            """
            print(err)
            raise
        finally:
            return results


def read_data(
    pipeline: beam.Pipeline,
    filepattern: str,
    dataparser: datasource.DataParser
) -> beam.PCollection:

    data = (
        pipeline
        | "Read Files" >> datasource.ReadFromCsv(filepattern)
        | "Parse Files" >> beam.Map(
            lambda row: parse_rows(dataparser, row)
        )
        | "Print Rows" >> beam.Map(print)
    )
    return data


def run_pipeline(
    filepattern: str,
    dataparser: datasource.DataParser,
    db_conn_obj: DatabaseConnection,
    dest_table: str
):

    schema = dataparser.schema
    columns = schema._fields
    min_batch_size = 10000
    max_batch_size = 100000
    with beam.Pipeline(options=options) as p:
        data = (
            p
            | "Read Files" >> datasource.ReadFromCsv(filepattern)
            | "Parse Files" >> beam.Map(
                lambda row: parse_rows(dataparser, row)
            )
            | "Map To Schema" >> beam.Map(lambda row: schema(**row)).with_output_types(schema)
            | "WriteToDB" >> WriteToPostgres(
                db_conn_obj,
                dest_table,
                columns,
                min_batch_size=min_batch_size,
                max_batch_size=max_batch_size
            )
            | "Print Rows" >> beam.Map(print)
        )

        print(data)


def run(options: pipeline_options.PipelineOptions):
    # NERRPipelineOptions has filepatterns associated by default.
    wq_filepattern = options.view_as(
        NERRPipelineOptions
    ).wq_filepattern
    nut_filepattern = options.view_as(
        NERRPipelineOptions
    ).nut_filepattern
    station_filepattern = options.view_as(
        NERRPipelineOptions
    ).station_filepattern
    met_filepattern = options.view_as(
        NERRPipelineOptions
    ).met_filepattern

    db_conn_obj = EnvironmentalDataConnection()

    run_pipeline(
        nut_filepattern,
        datasource.NutrientsDataParser(),
        db_conn_obj=db_conn_obj,
        dest_table='"NERRNutrients"'
    )
    run_pipeline(
        met_filepattern,
        datasource.MeteorologyDataParser(),
        db_conn_obj=db_conn_obj,
        dest_table='"NERRMeteorology"'
    )
    run_pipeline(
        wq_filepattern,
        datasource.WaterQualityDataParser(),
        db_conn_obj=db_conn_obj,
        dest_table='"NERRWaterQuality"'
    )
    run_pipeline(
        station_filepattern,
        datasource.SamplingStationsDataParser(),
        db_conn_obj=db_conn_obj,
        dest_table='"NERRSamplingStations"'
    )


# Run the pipeline
if __name__ == '__main__':
    options = pipeline_options.PipelineOptions(sys.argv[1:])
    run(options)
