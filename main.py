import time
import board
import busio
import adafruit_sgp30
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
from helpers import set_interval
import json
import os
import systemd.daemon


i2c = busio.I2C(board.SCL, board.SDA, frequency=100000)

# Create library object on our I2C port
sgp30 = adafruit_sgp30.Adafruit_SGP30(i2c)

print("SGP30 serial #", [hex(i) for i in sgp30.serial])

sgp30.iaq_init()
sgp30.set_iaq_baseline(0x8973, 0x8aae)

elapsed_sec = 0

influx = InfluxDBClient(os.environ['STATS_DB_HOST'], int(os.environ['STATS_DB_PORT']), os.environ['STATS_DB_USER'],
                        os.environ['STATS_DB_PASS'], os.environ['STATS_DB_DB'])

client = mqtt.Client()


def save_in_db():
    json_body = [
        {
            "measurement": "sgp30",
            "fields": {
                "eCO2": sgp30.eCO2,
                "TVOC": sgp30.TVOC,
            }
        }
    ]

    influx.write_points(json_body)


def publish_readings():
    client.publish("home/sensors/sgp30", json.dumps({
        "type": "sgp30",
        "state": {
            "eCO2": sgp30.eCO2,
            "TVOC": sgp30.TVOC,
        }
    }))


def publish_baselines():
    client.publish("home/sgp30/baseLines", json.dumps({
        "eCO2": sgp30.baseline_eCO2,
        "TVOC": sgp30.baseline_TVOC
    }), qos=0, retain=True)


def on_mqtt_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    client.subscribe("home/sgp30/baseLines")


def on_mqtt_message(client, userdata, msg):
    msgJson = json.loads(str(msg.payload.decode("utf-8", "ignore")))

    sgp30.set_iaq_baseline(msgJson.eCO2, msgJson.TVOC)

    client.unsubscribe("home/sgp30/baseLines")


def read_sensor():
    global elapsed_sec
    print("eCO2 = %d ppm \t TVOC = %d ppb" % (sgp30.eCO2, sgp30.TVOC))
    save_in_db()
    publish_readings()
    elapsed_sec += 1
    if elapsed_sec > 10:
        elapsed_sec = 0
        publish_baselines()


if __name__ == '__main__':
    set_interval(read_sensor, 1)

    client.on_connect = on_mqtt_connect
    client.on_message = on_mqtt_message

    client.connect(os.environ['MQTT_HOST'], int(os.environ['MQTT_PORT']), 60)

    systemd.daemon.notify('READY=1')

    client.loop_forever()
