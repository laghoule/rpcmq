#!/usr/bin/env python

# Written by Pascal Gauthier <pgauthier@onebigplanet.com>
# 03.09.2012 

import socket
import time
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


class ServerRPC:
    def __init__(self, amqp_server, virtualhost, credentials, amqp_exchange, amqp_rkey):
        "Connect to AMQP bus and request queue, and bind to exchange"

        ha = {}
        ha["x-ha-policy"]="all"

        while True:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server, credentials=credentials, virtual_host=virtualhost))
                self.channel = self.connection.channel()
            except socket.error, err:
                print "Connection error (%s), will try again in 5 sec..." % (err)
                time.sleep(5)
            except Exception, err:
                print "Error (%s), will try again in 5 sec..." % (err)
                self.connection.close()
            else:
                break

        self.result = self.channel.queue_declare(exclusive=True)
        self.amqp_queue = self.result.method.queue

        self.channel.queue_bind(exchange=amqp_exchange, queue=self.amqp_queue, routing_key=amqp_rkey)

    def __execute_cmd__(self, response_channel, method, properties, cmd):
        "Execute command in /opt/rpc-scripts path"

        self.rpc_cmd = "/opt/rpc-scripts/" + cmd
        self.response = os.system(self.rpc_cmd)

        self.syslog_msg = ("%s: os.system return status code %s") % (self.rpc_cmd, self.response)
        syslog.openlog("rpcmqd")
        syslog.syslog(self.syslog_msg)

        response_channel.basic_publish(exchange='', routing_key=properties.reply_to, properties=pika.BasicProperties(correlation_id=properties.correlation_id), body=str(self.response))
        response_channel.basic_ack(delivery_tag = method.delivery_tag)

        return self.response

    def consume_msg(self):
        "Consume AMQP message"

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.__execute_cmd__, queue=self.amqp_queue)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt, err:
            print "Keyboard interruption"
            self.connection.close()


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
        client = ServerRPC(amqp_server, virtualhost, credentials, amqp_exchange, amqp_rkey)
        client.consume_msg()
    else:
        raise ValueError(usage)


main()

