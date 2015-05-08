import os
import json
import time
import urlparse
from datetime import datetime, date
from decimal import Decimal

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import BINLOG
from kafka import SimpleProducer, KafkaClient
from kafka.common import LeaderNotAvailableError


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        serial = obj.isoformat()
        return serial
    if isinstance(obj, Decimal):
    	return float(obj)
    else:
    	print "Type '{}' for '{}' not serializable".format(obj.__class__, obj)
    	return None

def build_message(binlog_evt):
	schema = {'table': getattr(binlog_evt, 'schema', '') + "." + getattr(binlog_evt, 'table', '')};

	if binlog_evt.event_type == BINLOG.WRITE_ROWS_EVENT_V2:
		# Insert
		return {'event':'INSERT', 'headers':schema, 'data':binlog_evt.rows[0]['values']}

	elif binlog_evt.event_type == BINLOG.UPDATE_ROWS_EVENT_V2:
		# Update
		return {'event':'UPDATE', 'headers':schema, 'data':binlog_evt.rows[0]['after_values']}
	elif binlog_evt.event_type == BINLOG.DELETE_ROWS_EVENT_V2:
		# Delete
		return {'event':'DELETE', 'headers':schema, 'data':binlog_evt.rows[0]['values']}

	else:
		return None


kafka = KafkaClient("localhost:9092")

producer = SimpleProducer(kafka)
producer.send_messages("test", "test msg")

# To wait for acknowledgements
# ACK_AFTER_LOCAL_WRITE : server will wait till the data is written to
#                         a local log before sending response
# ACK_AFTER_CLUSTER_COMMIT : server will block until the message is committed
#                            by all in sync replicas before sending a response
producer = SimpleProducer(kafka, async=False,
                          req_acks=SimpleProducer.ACK_AFTER_LOCAL_WRITE,
                          ack_timeout=2000)


conf = urlparse.urlparse(os.environ['RDS_URL'])
mysql_settings = {'host': conf.hostname, 
					'port': conf.port, 
					'user': conf.username, 
					'passwd': conf.password}

# Connect to Mysql replication stream
print "Connecting to Mysql at {}...".format(mysql_settings['host'])
stream = BinLogStreamReader(connection_settings = mysql_settings, server_id=100, resume_stream=False,
                                blocking=True)
print "connected. Listening for changes..."

for evt in stream:
	evt.dump()
	msg = build_message(evt)
	if msg:
		try:
			response = producer.send_messages(msg['headers']['table'], json.dumps(msg, default=json_serial))
		except LeaderNotAvailableError:
			time.sleep(1)
			response = producer.send_messages(msg['headers']['table'], json.dumps(msg, default=json_serial))
		# TODO: Test response.error
		# TODO: Store replication stream pos

stream.close()
