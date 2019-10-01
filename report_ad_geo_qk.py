from pyspark import SparkContext
from quadkey_template_db import QuadkeyTemplateDB


def line2regions(record):
    global qk_db

    if qk_db is None:
        qk_db = QuadkeyTemplateDB("./qk_cn.csv")

    fields = record.split(",")
    quadkey = fields[-1]
    log_type = fields[3]
    ad_id = fields[4]
    imei = fields[15]

    return "{},{},{}".format(ad_id, log_type, imei), qk_db.lookup_regions(quadkey)


def make_pairs(x):
    fields = x[0].split(",")
    return "{},{}".format(fields[0], x[1]), "{},{}".format(fields[1], fields[2])


def calculate_stats(iter):
    (count_imp, count_click) = (0, 0)
    imei_imp = set()
    imei_click = set()

    for line in iter:
        record = line.split(",")
        (log_type, imei) = record[0], record[1]

        if log_type == 1:
            count_imp = count_imp + 1
            imei_imp.add(imei)
        else:
            count_click = count_click + 1
            imei_click.add(imei)

    return count_imp, len(imei_imp), count_click, len(imei_click)


if __name__ == "__main__":
    sc = SparkContext()

    qk_db = None

    records = sc.textFile("hdfs://localhost/user/cloudera/audi_case_study/location_info_added")

    result = records.map(line2regions) \
                    .flatMapValues(lambda x: x) \
                    .map(make_pairs) \
                    .groupByKey() \
                    .mapValues(calculate_stats)

    result.persist()

    result.saveAsTextFile("hdfs://localhost/user/cloudera/audi_case_study/report_ad_geo")

    for item in result.collect():
        print(item)