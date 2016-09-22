import boto3
import datetime
import json
import pdb
import random
import time

from pprint import pprint


if __name__ == "__main__":

    blueprint = {
        "meta_id": None,
        "node_id": None,
        "sensor": None,
        "feature_of_interest": None,
        "data": None,
        "datetime": None,
    }

    node_to_sensors = {
        "node_dev_1": ["sensor_dev_1", "sensor_dev_4"],
        "node_dev_2": ["sensor_dev_2", "sensor_dev_3"],
    }

    sensors_to_foi = {
        "sensor_dev_1": ["mag_x", "mag_y"],
        "sensor_dev_2": ["humidity"],
        "sensor_dev_3": ["n2", "co2"],
        "sensor_dev_4": ["temp", "mag_z", "oxygen"],
    }

    kinesis_client = boto3.client("kinesis")

    while True:

        # At this rate it will create approximately 6000 dummy records per day.
        time.sleep(2)

        blueprint["meta_id"] = random.randint(0, 100000)
        blueprint["node_id"] = random.choice(["node_dev_1", "node_dev_2"])
        blueprint["sensor"] = random.choice(node_to_sensors[blueprint["node_id"]])
        blueprint["datetime"] = str(datetime.datetime.utcnow().isoformat())
        blueprint["data"] = {}

        for prop in sensors_to_foi[blueprint["sensor"]]:
            blueprint["data"][prop] = random.random()

        payload = json.dumps(blueprint).encode("ascii")

        # print "==============================================="
        # print "SEND"
        pprint(blueprint)
        # print "RECIEVE"
        # pprint
        kinesis_client.put_record(
            StreamName="TestStream",
            PartitionKey="arbitrary",
            Data=payload,
        )
        # print "==============================================="
