import csv
import threading
from time import sleep

from kafka import KafkaProducer


class ZephyProducerECG(threading.Thread):

    def __init__(self):
        super(ZephyProducerECG, self).__init__()
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    def start_streaming(self):
        with open("/home/sartharion/Bureau/5-mcluet/ZEPHYR/2020_02_07__13_18_12_ECG.csv",
                  newline='') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                string = ';'.join(row) + "\n"
                self.producer.send('Zephyr', value=string.encode(), key="ECG".encode(),partition=2)
                print("sent ECG")
                sleep(0.004)

    def run(self):
        self.start_streaming()