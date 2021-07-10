# python -m unittest tests/datasource_test.py
import unittest
# from apache_beam.testing.util import assert_that, equal_to
from datetime import datetime

from tests import input_data
from datasource import (
    WaterQualityDataParser,
    NutrientsDataParser,
    MeteorologyDataParser,
    SamplingStationsDataParser
)


class DataParserTest(unittest.TestCase):

    def test_field_parsers(self):
        parser = WaterQualityDataParser()

        # Date parser
        self.assertEqual(parser.date_parse(""), None)
        self.assertEqual(
            parser.date_parse("   1/8/2007 0:00   "),
            str(datetime(2007, 1, 8, 0, 0))
        )
        self.assertEqual(
            parser.measurement_parse(""),
            None
        )
        self.assertEqual(
            parser.measurement_parse("0.0001    "),
            0.0001
        )
        self.assertEqual(
            parser.string_parse(""),
            None
        )
        self.assertEqual(
            parser.string_parse(" A   random string 123  "),
            "A   random string 123"
        )
        self.assertEqual(
            parser.integer_parse(""),
            None
        )

    def test_row_parsers(self):
        """
            Tests that the parsers are working
            as expected using real data.
        """

        # A list of three el tuples that contain the parser,
        # test data and expected output
        parsers_data_expected = [
            (
                WaterQualityDataParser(),
                input_data.WATERQUALITY_REAL_ROW,
                input_data.WATERQUALITY_PARSED_REAL_ROW
            ),
            (
                NutrientsDataParser(),
                input_data.NUTRIENTS_REAL_ROW,
                input_data.NUTRIENTS_PARSED_REAL_ROW
            ),
            (
                NutrientsDataParser(),
                input_data.NUTRIENTS_EMPTY_ROW,
                input_data.NUTRIENTS_PARSED_EMPTY_ROW
            ),
            (
                MeteorologyDataParser(),
                input_data.METEOROLOGY_REAL_ROW,
                input_data.METEOROLOGY_PARSED_REAL_ROW
            ),
            (
                SamplingStationsDataParser(),
                input_data.SAMPLINGSTATIONS_REAL_ROW,
                input_data.SAMPLINGSTATIONS_PARSED_REAL_ROW
            )
        ]

        for parser, data, expected in parsers_data_expected:
            output = parser.parse_row(data)
            self.assertEqual(output, expected)


if __name__ == '__main__':
    unittest.main()
