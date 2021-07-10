"""File with transforms to consume NERR data into Beam pipelines."""

import csv
import io
from abc import ABC, abstractmethod
from typing import Dict, Union
from typing import NamedTuple, Optional

import apache_beam as beam
from apache_beam.io import fileio
from dateutil.parser import parse
from numbers import Number


"""
    Schemas for each Dataset used with
    beam to make sure the data follows
    the expected structure...
"""


class WaterQualitySchema(NamedTuple):
    StationCode: str
    isSWMP: Optional[str]
    DateTimeStamp: str
    Historical: Optional[int]
    ProvisionalPlus: Optional[int]
    F_Record: Optional[str]
    Temp: Optional[float]
    F_Temp: Optional[str]
    SpCond: Optional[float]
    F_SpCond: Optional[str]
    Sal: Optional[float]
    F_Sal: Optional[str]
    DO_Pct: Optional[float]
    F_DO_Pct: Optional[str]
    DO_mgl: Optional[float]
    F_DO_mgl: Optional[str]
    Depth: Optional[float]
    F_Depth: Optional[str]
    cDepth: Optional[float]
    F_cDepth: Optional[str]
    Level: Optional[float]
    F_Level: Optional[str]
    cLevel: Optional[float]
    F_cLevel: Optional[str]
    pH: Optional[float]
    F_pH: Optional[str]
    Turb: Optional[float]
    F_Turb: Optional[str]
    ChlFluor: Optional[float]
    F_ChlFluor: Optional[str]


class NutrientsSchema(NamedTuple):
    StationCode: str
    isSWMP: str
    DateTimeStamp: str
    Historical: int
    ProvisionalPlus: int
    CollMethd: int
    REP: int
    F_Record: str
    PO4F: float
    F_PO4F: str
    NH4F: float
    F_NH4F: str
    NO2F: float
    F_NO2F: str
    NO3F: float
    F_NO3F: str
    NO23F: float
    F_NO23F: str
    CHLA_N: float
    F_CHLA_N: str


class MeteorologySchema(NamedTuple):
    StationCode: str
    isSWMP: Optional[int]
    DateTimeStamp: str
    Historical: Optional[int]
    ProvisionalPlus: Optional[int]
    Frequency: Optional[int]
    F_Record: Optional[str]
    ATemp: Optional[float]
    F_ATemp: Optional[str]
    RH: Optional[float]
    F_RH: Optional[str]
    BP: Optional[float]
    F_BP: Optional[str]
    WSpd: Optional[float]
    F_WSpd: Optional[str]
    MaxWSpd: Optional[float]
    F_MaxWSpd: Optional[str]
    MaxWSpdT: Optional[str]
    Wdir: Optional[float]
    F_Wdir: Optional[str]
    SDWDir: Optional[float]
    F_SDWDir: Optional[str]
    TotPAR: Optional[float]
    F_TotPAR: Optional[str]
    TotPrcp: Optional[float]
    F_TotPrcp: Optional[str]
    TotSoRad: Optional[str]
    F_TotSoRad: Optional[str]


class SamplingStationsSchema(NamedTuple):

    NERRSiteID: str
    StationCode: str
    StationName: Optional[str]
    LatLong: Optional[str]
    Latitude: Optional[float]
    Longitude: Optional[float]
    Status: Optional[str]
    ActiveDates: Optional[str]
    State: Optional[str]
    ReserveName: Optional[str]
    RealTime: Optional[str]
    HADSID: Optional[str]
    GMTOffset: Optional[int]
    StationType: Optional[int]
    Region: Optional[int]
    isSWMP: Optional[str]
    ParametersReported: Optional[str]


class DataParser(ABC):
    schema = None
    """
        This is a Factory class in charge of processing
        the row objects returned by the CSV readers.
    """

    @abstractmethod
    def row_parser(self, row: Dict):
        """
            Method that other DataParser classes should have to parse
            row objects
        """
        pass

    def parse_row(self, row: Dict) -> Union[beam.Row, None]:
        """
            Method in charge of running the parsing operation.
        """

        # Take care of exceptions here?
        if not row:
            return None
        else:
            results = self.row_parser(row)
            return results

    # General functions used to parse different data types
    def measurement_parse(self, val: str) -> Union[Number, None]:
        val = val.strip()
        return float(val) if val else None

    def date_parse(self, val: str) -> Union[str, None]:
        val = val.strip()
        # Dates have the month first instead of day...
        return str(parse(val, dayfirst=False)) if val else None

    def integer_parse(self, val: str) -> Union[int, None]:
        val = val.strip()
        return int(val) if val else None

    def string_parse(self, val: str) -> Union[str, None]:
        val = val.strip()
        return str(val) if val else None


class WaterQualityDataParser(DataParser):
    schema = WaterQualitySchema

    def row_parser(self, row: Dict) -> Dict:
        return dict(
            StationCode=self.string_parse(row.get("StationCode", "")),
            isSWMP=self.string_parse(row.get("isSWMP", "")),
            DateTimeStamp=self.date_parse(row.get("DateTimeStamp", "")),
            Historical=self.integer_parse(row.get("Historical", "")),
            ProvisionalPlus=self.integer_parse(row.get("ProvisionalPlus", "")),
            F_Record=self.string_parse(row.get("F_Record", "")),
            Temp=self.measurement_parse(row.get("Temp", "")),
            F_Temp=self.string_parse(row.get("F_Temp", "")),
            SpCond=self.measurement_parse(row.get("SpCond", "")),
            F_SpCond=self.string_parse(row.get("F_SpCond", "")),
            Sal=self.measurement_parse(row.get("Sal", "")),
            F_Sal=self.string_parse(row.get("F_Sal", "")),
            DO_Pct=self.measurement_parse(row.get("DO_Pct", "")),
            F_DO_Pct=self.string_parse(row.get("F_DO_Pct", "")),
            DO_mgl=self.measurement_parse(row.get("DO_mgl", "")),
            F_DO_mgl=self.string_parse(row.get("F_DO_mgl", "")),
            Depth=self.measurement_parse(row.get("Depth", "")),
            F_Depth=self.string_parse(row.get("F_Depth", "")),
            cDepth=self.measurement_parse(row.get("cDepth", "")),
            F_cDepth=self.string_parse(row.get("F_cDepth", "")),
            Level=self.measurement_parse(row.get("Level", "")),
            F_Level=self.string_parse(row.get("F_Level", "")),
            cLevel=self.measurement_parse(row.get("cLevel", "")),
            F_cLevel=self.string_parse(row.get("F_cLevel", "")),
            pH=self.measurement_parse(row.get("pH", "")),
            F_pH=self.string_parse(row.get("F_pH", "")),
            Turb=self.measurement_parse(row.get("Turb", "")),
            F_Turb=self.string_parse(row.get("F_Turb", "")),
            ChlFluor=self.measurement_parse(row.get("ChlFluor", "")),
            F_ChlFluor=self.string_parse(row.get("F_ChlFluor", ""))
        )


class NutrientsDataParser(DataParser):
    schema = NutrientsSchema

    def row_parser(self, row: Dict) -> Dict:
        return dict(
            StationCode=self.string_parse(row.get("StationCode", "")),
            isSWMP=self.string_parse(row.get("isSWMP", "")),
            DateTimeStamp=self.date_parse(row.get("DateTimeStamp", "")),
            Historical=self.integer_parse(row.get("Historical", "")),
            ProvisionalPlus=self.integer_parse(row.get("ProvisionalPlus", "")),
            CollMethd=self.integer_parse(row.get("CollMethd", "")),
            REP=self.integer_parse(row.get("REP", "")),
            F_Record=self.string_parse(row.get("F_Record", "")),
            PO4F=self.measurement_parse(row.get("PO4F", "")),
            F_PO4F=self.string_parse(row.get("F_PO4F", "")),
            NH4F=self.measurement_parse(row.get("NH4F", "")),
            F_NH4F=self.string_parse(row.get("F_NH4F", "")),
            NO2F=self.measurement_parse(row.get("NO2F", "")),
            F_NO2F=self.string_parse(row.get("F_NO2F", "")),
            NO3F=self.measurement_parse(row.get("NO3F", "")),
            F_NO3F=self.string_parse(row.get("F_NO3F", "")),
            NO23F=self.measurement_parse(row.get("NO23F", "")),
            F_NO23F=self.string_parse(row.get("F_NO23F", "")),
            CHLA_N=self.measurement_parse(row.get("CHLA_N", "")),
            F_CHLA_N=self.string_parse(row.get("F_CHLA_N", ""))
        )


class MeteorologyDataParser(DataParser):
    schema = MeteorologySchema

    def row_parser(self, row: Dict) -> Dict:
        return dict(
            StationCode=self.string_parse(row.get("StationCode", "")),
            isSWMP=self.string_parse(row.get("isSWMP", "")),
            DateTimeStamp=self.date_parse(row.get("DatetimeStamp", "")),  # Notice the lowercase t Datetime...
            Historical=self.integer_parse(row.get("Historical", "")),
            ProvisionalPlus=self.integer_parse(row.get("ProvisionalPlus", "")),
            Frequency=self.integer_parse(row.get("Frequency", "")),
            F_Record=self.string_parse(row.get("F_Record", "")),
            ATemp=self.measurement_parse(row.get("ATemp", "")),
            F_ATemp=self.string_parse(row.get("F_ATemp", "")),
            RH=self.measurement_parse(row.get("RH", "")),
            F_RH=self.string_parse(row.get("F_RH", "")),
            BP=self.measurement_parse(row.get("BP", "")),
            F_BP=self.string_parse(row.get("F_BP", "")),
            WSpd=self.measurement_parse(row.get("WSpd", "")),
            F_WSpd=self.string_parse(row.get("F_WSpd", "")),
            MaxWSpd=self.measurement_parse(row.get("MaxWSpd", "")),
            F_MaxWSpd=self.string_parse(row.get("F_MaxWSpd", "")),
            MaxWSpdT=self.string_parse(row.get("MaxWSpdT", "")),
            Wdir=self.measurement_parse(row.get("Wdir", "")),
            F_Wdir=self.string_parse(row.get("F_Wdir", "")),
            SDWDir=self.measurement_parse(row.get("SDWDir", "")),
            F_SDWDir=self.string_parse(row.get("F_SDWDir", "")),
            TotPAR=self.measurement_parse(row.get("TotPAR", "")),
            F_TotPAR=self.string_parse(row.get("F_TotPAR", "")),
            TotPrcp=self.measurement_parse(row.get("TotPrcp", "")),
            F_TotPrcp=self.string_parse(row.get("F_TotPrcp", "")),
            TotSoRad=self.string_parse(row.get("TotSoRad", "")),
            F_TotSoRad=self.string_parse(row.get("F_TotSoRad", ""))
        )


class SamplingStationsDataParser(DataParser):
    schema = SamplingStationsSchema

    def row_parser(self, row: Dict) -> Dict:
        return dict(
            NERRSiteID=self.string_parse(row.get("NERR Site ID ", "")),
            StationCode=self.string_parse(row.get("Station Code", "")),
            StationName=self.string_parse(row.get("Station Name", "")),
            LatLong=self.string_parse(row.get("Lat Long", "")),
            Latitude=self.measurement_parse(row.get("Latitude ", "")),
            Longitude=self.measurement_parse(row.get(" Longitude", "")),  # Notice the spaces before the names...
            Status=self.string_parse(row.get(" Status", "")),
            ActiveDates=self.string_parse(row.get(" Active Dates", "")),
            State=self.string_parse(row.get(" State", "")),
            ReserveName=self.string_parse(row.get(" Reserve Name", "")),
            RealTime=self.string_parse(row.get("Real Time", "")),
            HADSID=self.string_parse(row.get("HADSID", "")),
            GMTOffset=self.integer_parse(row.get("GMT Offset", "")),
            StationType=self.integer_parse(row.get("Station Type", "")),
            Region=self.integer_parse(row.get("Region", "")),
            isSWMP=self.string_parse(row.get("isSWMP", "")),
            ParametersReported=self.string_parse(
                row.get("Parameters Reported", "")
            )
        )


class ReadAllFromCsv(beam.PTransform):
    """
        Matches the filenames returned in a pcollection by
        the ReadFromCsv PTransform below.
    """
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return (
            pcoll
            | 'MatchAll' >> fileio.MatchAll()
            # Allows parallel processing
            | beam.Reshuffle()
            | 'ReadEach' >> fileio.ReadMatches()
            # Take each file, read as csv and outputs list
            # of dictionaries
            | beam.FlatMap(
                lambda rfile: csv.DictReader(
                    io.TextIOWrapper(rfile.open())
                )
            )
        )


class ReadFromCsv(beam.PTransform):
    """
        Creates a PCollection from the filepattern
        provided.
    """
    def __init__(self, filepattern: str):
        self.filepattern = filepattern

    def expand(self, pipeline: beam.Pipeline) -> beam.PCollection:
        return (
            pipeline
            | beam.Create([self.filepattern])
            | ReadAllFromCsv()
        )
