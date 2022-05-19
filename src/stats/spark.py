from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


class SparkController:
    context = None
    configuration = None

    def __init__(self, config: dict):
        self.config = config

    def _set_spark_configuration(self):
        self.configuration = SparkConf()
        for k, v in self.config.items():
            self.configuration.set(k, v)


    def _set_context(self):
        if not self.configuration:
            self._set_spark_configuration()
        self.context = SparkContext(conf=self.configuration)

    def set_session(self):
        if not self.context:
            self._set_context()
        self.session = SparkSession(self.context).builder.getOrCreate()
