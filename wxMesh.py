# weewx driver that reads data from MQTT subscription
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.
#
# See http://www.gnu.org/licenses/

# To use this driver, put this file in the weewx user directory, then make
# the following changes to weewx.conf:
#
# [Station]
#     station_type = wxMesh
# [wxMesh]
#     host = localhost           # MQTT broker hostname
#     topic = weather/+          # topic
#     driver = user.wxMesh
#
# If the variables in the file have names different from those in weewx, then
# create a mapping such as this:
#
# [wxMesh]
#     ...
#     [[label_map]]
#         temp = outTemp
#         humi = outHumidity
#         in_temp = inTemp
#         in_humid = inHumidity

from __future__ import with_statement
import logging
import time
import queue
import paho.mqtt.client as mqtt
import json
from decimal import Decimal
import weewx.drivers

log = logging.getLogger(__name__)

DRIVER_NAME = 'wxMesh'
DRIVER_VERSION = "0.3"

def _get_as_float(d, s):
    v = None
    if s in d:
        try:
            v = float(d[s])
        except ValueError:
            log.error("cannot read value for '%s': of type %s" % (s, type(d[s])))
        except e:
            log.error("unhandled error '%s': for data %s of type %s" % (e, s, type(d[s])))
    return v

def loader(config_dict, engine):
    return wxMesh(**config_dict[DRIVER_NAME])

class wxMesh(weewx.drivers.AbstractDevice):
    """weewx driver that reads data from a file"""

    def __init__(self, **stn_dict):
      # where to find the data file
      self.host = stn_dict.get('host', 'localhost')
      self.topic = stn_dict.get('topic', 'weather')
      self.username = stn_dict.get('username', 'no default')
      self.password = stn_dict.get('password', 'no default')
      self.client_id = stn_dict.get('client', 'wxclient') # MQTT client id - adjust as desired
      
      # how often to poll the weather data file, seconds
      self.poll_interval = float(stn_dict.get('poll_interval', 5.0))
      # mapping from variable names to weewx names
      self.label_map = stn_dict.get('label_map', {})

      log.info("MQTT host is %s" % self.host)
      log.info("MQTT topic is %s" % self.topic)
      log.info("MQTT client is %s" % self.client_id)
      log.info("polling interval is %s" % self.poll_interval)
      log.info('label map is %s' % self.label_map)
      
      self.payloadQueue = queue.Queue()
      self.connected = False

      self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=self.client_id, protocol=mqtt.MQTTv31)

      # TODO - need some reconnect on disconnect logic?
      #self.client.on_disconnect = self.on_disconnect
      self.client.on_connect = self.on_connect
      self.client.on_message = self.on_message

      self.client.username_pw_set(self.username, self.password)

      self.client.connect(self.host, 1883, 60)
      # TODO is this a good idea?
      # while self.connected != True:
      time.sleep(4)
        # log.debug("Attempting connect to %s...\n" % self.host)
      
      log.debug("Connected")

      self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
      if rc == 0:
        self.client.subscribe(self.topic, qos=1)
        log.debug("Subscribed to %s" % self.topic)
        self.connected = True
      else:
        log.error("Failed to connect to MQTT broker rc = ", rc)
        
    # The callback for when a PUBLISH message is received from the MQTT server.
    def on_message(self, client, userdata, msg):
      self.payloadQueue.put(msg.payload.decode('utf-8'))
      log.debug("Added to queue of %d message %s" % (self.payloadQueue.qsize(), msg.payload))

    def closePort(self):
      self.client.disconnect()
      self.client.loop_stop()

    def genLoopPackets(self):
      while True:
	# read whatever values we can get from the MQTT broker
        log.debug("Queue of %d entries" % self.payloadQueue.qsize())
        log.debug("Waiting for non-empty queue")
        while not self.payloadQueue.empty(): 
          msg = self.payloadQueue.get(block=True, timeout=3) # block until something gets queued
          try:
            log.debug("Working on queue of size %d with payload : %s" % (self.payloadQueue.qsize(), msg))
            data = json.loads(msg)
          except Exception as e:
            log.error("Failed loading JSON: type %s msg %s exception: %s" % (type(msg), msg, repr(e)))
            continue
          data['TIME'] = int(time.time()) # time from station is not yet reliable - replace it
          for key, value in data.items(): # Python 2.7! 3 uses items()
            log.debug("key: "+key+" value: "+str(data[key])+" type:"+str(type(data[key])))
            
          # map the data into a weewx loop packet
          _packet = {'usUnits': weewx.METRIC}
          for vname in data:
            if vname in self.label_map:
              _packet[self.label_map.get(vname, vname)] = _get_as_float(data, str(vname))

          yield _packet

        log.debug("Sleeping for %d" % self.poll_interval)
        time.sleep(self.poll_interval)

      self.client.disconnect()
      self.client.loop_stop()
        
    @property
    def hardware_name(self):
        return "wxMesh"
