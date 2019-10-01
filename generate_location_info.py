from pyspark import SparkContext
from geoip2.database import Reader
import quadkey


def add_location_info(record):
    global reader
    if reader is None:
        reader = Reader("/home/cloudera/spark-2.4.3-bin-hadoop2.6/maxmind/GeoLite2-City.mmdb")

    ip = record.split(",")[13]

    try:
        response = reader.city(ip)
        country = response.country.name
        city = response.city.name
        latitude = response.location.latitude
        longitude = response.location.longitude
        qk = quadkey.from_geo((latitude, longitude), 15)
        acc_num_good_records.add(1)
        return "{},{},{},{},{},{}".format(record, country, city, latitude, longitude, qk.key)

    except:
        acc_num_bad_records.add(1)
        return "-----"


if __name__ == "__main__":
    sc = SparkContext()

    outputPath = "hdfs://localhost/user/cloudera/audi_case_study/location_info_added"

    reader = None

    acc_num_bad_records = sc.accumulator(0)

    acc_num_good_records = sc.accumulator(0)

    records = sc.textFile("hdfs://localhost/user/cloudera/audi_case_study/data/")

    records.map(add_location_info) \
           .filter(lambda x: x != "-----") \
           .saveAsTextFile(outputPath)

    print("Number of good records: {}, Number of bad records: {}".format(acc_num_good_records.value, acc_num_bad_records.value))
