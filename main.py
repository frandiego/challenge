from src.stats.webtraffic import WebTrafficStats
from src.stats.spark import SparkController
from yaml import load, FullLoader

with open(r'config.yml') as file:
    config = load(file, Loader=FullLoader)

if __name__ == "__main__":
    spark = SparkController(config['spark'])
    spark.set_session()
    web_traffic_stats = WebTrafficStats(config['webtraffic_pipeline'], spark.session)
    web_traffic_stats.run_pipeline()
    spark.session.read.parquet(config['webtraffic_pipeline']['output']['path']).show()
    spark.session.stop()