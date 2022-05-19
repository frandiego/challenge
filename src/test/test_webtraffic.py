from src.stats.webtraffic import WebTrafficStats
from src.stats.spark import SparkController
from pyspark.sql import SparkSession, DataFrame
from yaml import load, FullLoader
from unittest import TestCase
import os


with open(r'config.yml') as file:
    config = load(file, Loader=FullLoader)


class TestWebTrafficStats(TestCase):
    data_path = config['test']['data_path']
    spark = SparkController(config['test']['spark'])
    spark.set_session()
    wts = WebTrafficStats(config.get('webtraffic_pipeline'), spark.session)

    @classmethod
    def expected_and_output(cls, input_path: str, output_path: str, function) -> (DataFrame, DataFrame):
        df = cls.spark.session.read.json(input_path)
        return cls.spark.session.read.json(output_path), function(df)

    @staticmethod
    def same_data(df_a: DataFrame, df_b: DataFrame) -> bool:
        dt_a , dt_b = df_a.toPandas(), df_b.toPandas()
        return dt_b[dt_a.columns].compare(dt_a, keep_equal=False).empty

    @classmethod
    def check_expected_output(cls, input_path: str, output_path: str, function) -> bool:
        dfexp, dfout = cls.expected_and_output(input_path, output_path, function)
        return cls.same_data(dfexp, dfout)

    @classmethod
    def real_path(cls, filename: str) -> str:
        return os.path.join(cls.data_path, filename)

    @classmethod
    def test_tidy(cls):
        return cls.check_expected_output(cls.real_path('00_read.json'), cls.real_path('01_tidy.json'), cls.wts.tidy)

    @classmethod
    def test_flat(cls):
        return cls.check_expected_output(cls.real_path('01_tidy.json'), cls.real_path('02_flat.json'), cls.wts.flat)

    @classmethod
    def test_count(cls):
        return cls.check_expected_output(cls.real_path('02_flat.json'), cls.real_path('03_count.json'), cls.wts.count)

    @classmethod
    def test_stats(cls):
        return cls.check_expected_output(cls.real_path('03_count.json'), cls.real_path('04_stats.json'), cls.wts.stats)

    @classmethod
    def test_output(cls):
        return cls.check_expected_output(cls.real_path('04_stats.json'),cls.real_path('05_output.json'), cls.wts.output)
