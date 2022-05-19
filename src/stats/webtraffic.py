from json import loads

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, IntegerType

from . import DIMENSION_COLS, CREATED_COLS, METRICS_COLS


def jsonfile_to_schema(path):
    with open(path) as file:
        return StructType.fromJson(loads(file.read()))


# udf to get the lenght of unique values in a iterator
n_unique = f.udf(lambda x: len(set(x)), IntegerType())

# udf to sum the number of consent in a list of tokens
n_enabled = lambda i: len(loads(i).get('purposes').get('enabled'))
n_consent = f.udf(lambda i: sum(map(n_enabled, i)), IntegerType())


class WebTrafficStats:

    def __init__(self, config: dict, spark_session: SparkSession):
        self.config = config
        self.spark_session = spark_session

    def read(self) -> DataFrame:
        cnf = self.config['input']
        schema = jsonfile_to_schema(cnf['schema'])
        return self.spark_session.read.format(cnf['format']).load(cnf['path'], schema=schema)

    def tidy(self, df: DataFrame) -> DataFrame:
        return df.drop_duplicates(['id']) \
            .select(['id', 'datehour', 'domain', 'type'] +
                    [f.col('user.country').alias('country'), f.col('user.id').alias('uid'),
                     f.col('user.token').alias('token')])

    def flat(self, df: DataFrame) -> DataFrame:
        return df.groupBy(DIMENSION_COLS + ['type', 'uid']) \
            .agg(f.collect_set('id').alias('ids'), f.collect_list('token').alias('tokens'))

    def count(self, df: DataFrame) -> DataFrame:
        return df.select(DIMENSION_COLS + ['type', 'uid'] +
                         [n_unique(f.col('ids')).alias('n_events'),
                          n_consent(f.col('tokens')).alias('n_consent')])

    def stats(self, df: DataFrame) -> DataFrame:
        return df.groupBy(DIMENSION_COLS).pivot('type') \
            .agg(f.sum('n_events').alias('total_events'),
                 f.sum('n_consent').alias('total_consent'),
                 f.avg('n_events').alias('avg_events'))

    def output(self, df: DataFrame) -> DataFrame:
        return df.na.fill(0).toDF(*CREATED_COLS).select(*DIMENSION_COLS + METRICS_COLS)


    def change_schema(self, df: DataFrame, schema: dict):
        return self.spark_session.createDataFrame(df.rdd, schema=schema)

    def read_and_process(self) -> DataFrame:
        df = self.read()
        df = self.tidy(df)
        df = self.flat(df)
        df = self.count(df)
        df = self.stats(df)
        return self.output(df)

    def run_pipeline(self):
        cnf = self.config['output']
        df = self.read_and_process()
        df = self.change_schema(df, jsonfile_to_schema(cnf['schema']))
        df.write.format(cnf['format']).mode(cnf['mode']).save(cnf['path'])
