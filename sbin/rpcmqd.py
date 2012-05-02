#!/usr/bin/env python

# Written by Pascal Gauthier <pgauthier@onebigplanet.com>
# 03.09.2012 

import sys
import os
import pika
import syslog
import ConfigParser

usage = "Usage: rpcmqd.py -c config_file"

def read_config(config_file, section, var):
    "Read config and return value"
    config = ConfigParser.RawConfigParser()
    config.read(config_file)

    value = config.get(section, var)
    return value


def execute_cmd(ch, method, props, cmd):
    "Execute command in /opt/rpc-scripts path"
    rpc_cmd = "/opt/rpc-scripts/%s" % (cmd,)
    response = os.system(rpc_cmd)

    syslog_msg = ("os.system %s return status code %s") % (rpc_cmd, response)
    syslog.openlog("rpcmqd")
    syslog.syslog(syslog_msg)

    ch.basic_publish(exchange='', routing_key=props.reply_to, properties=pika.BasicProperties(correlation_id=props.correlation_id), body=str(response))
    ch.basic_ack(delivery_tag = method.delivery_tag)


def amqp_consume(amqp_server, virtualhost, credentials, amqp_exchange, amqp_rkey):
    "Create and assign queue to an exchange"
    ha = {}
    ha["x-ha-policy"]="all"

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server, credentials=credentials, virtual_host=virtualhost))
    channel = connection.channel()

    result = channel.queue_declare(exclusive=True)
    amqp_queue = result.method.queue

    channel.queue_bind(exchange=amqp_exchange, queue=amqp_queue, routing_key=amqp_rkey)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(execute_cmd, queue=amqp_queue)
    channel.start_consuming()
    connection.close()


def main():
    "Main function"
    if len(sys.argv) >= 3 and sys.argv[1] == "-c":
        if os.path.exists(sys.argv[2]):
            config_file = sys.argv[2]
            amqp_server = read_config(config_file, "main", "server")
            amqp_exchange = read_config(config_file, "queue", "exchange")
            amqp_rkey = read_config(config_file, "queue", "routing_key")
            virtualhost = read_config(config_file, "queue", "virtualhost")
            username = read_config(config_file, "queue", "username")
            password = read_config(config_file, "queue", "password")
            credentials = pika.PlainCredentials(username, password)
        else:
            err_msg = "File %s don't exist" % (sys.argv[2],)
            raise ValueError(err_msg)
        amqp_consume(amqp_server, virtualhost, credentials, amqp_exchange, amqp_rkey)
    else:
        raise ValueError(usage)


main()

