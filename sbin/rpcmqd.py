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
import argparse

__metaclass__ = type

class ServerRPC:
    def __init__(self, amqp_server, virtualhost, credentials, amqp_exchange, amqp_rkey, ssl_info):
        "Connect to AMQP bus and request queue, and bind to exchange"

        while True:
            try:
                if ssl_info.get('enable') == "on":
                    #self.ssl_options = { 'ca_certs': ssl_info.get('cacert'), 
                    #                    'certfile': ssl_info.get('cert'), 'keyfile': ssl_info.get('key') }
                    #self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server,
                    #                            credentials=credentials, virtual_host=virtualhost, ssl=True,
                    #                            ssl_options=self.ssl_options)) 
                    print "AMQPS support broken right now..."
                    self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server,
                                            credentials=credentials, virtual_host=virtualhost))
                elif ssl_info.get('enable') == "off":
                    self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server,
                                            credentials=credentials, virtual_host=virtualhost))
                else:
                    raise Exception("ssl enable = on/off")
                self.channel = self.connection.channel()
            except socket.error, err:
                print """Connection error (%s), will try again 
                        in 5 sec...""" % (err)
                time.sleep(5)
            except Exception, err:
                print "Error (%s), will try again in 5 sec..." % (err)
                self.connection.close()
            else:
                break

        self.result = self.channel.queue_declare(exclusive=True)
        self.amqp_queue = self.result.method.queue

        self.channel.queue_bind(exchange=amqp_exchange, queue=self.amqp_queue,
                       routing_key=amqp_rkey)

    def __execute_cmd__(self, response_channel, method, properties, cmd):
        "Execute command in /opt/rpc-scripts path"

        self.rpc_cmd = "/opt/rpc-scripts/" + cmd
        self.response = os.system(self.rpc_cmd)

        self.syslog_msg = ("%s: os.system return status code %s") % (self.rpc_cmd, self.response)
        syslog.openlog("rpcmqd")
        syslog.syslog(self.syslog_msg)

        response_channel.basic_publish(exchange='', routing_key=properties.reply_to, 
                            properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                            body=str(self.response))

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

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", dest="config",
                help="path to config FILE", metavar="FILE")

    args = parser.parse_args()
    
    if not args.config:
        parser.print_help()
        sys.exit(1)

    config_file = open(args.config, "r")
    config = ConfigParser.RawConfigParser()
    config.readfp(config_file)

    amqp_server = config.get("main", "server")
    amqp_exchange = config.get("rpc-context", "exchange")
    amqp_rkey = config.get("rpc-context", "routing_key")
    virtualhost = config.get("rpc-context", "virtualhost")
    username = config.get("rpc-context", "username")
    password = config.get("rpc-context", "password")
    ssl_enable = config.get("ssl", "enable")
    cacertfile = config.get("ssl", "cacertfile")
    certfile = config.get("ssl", "certfile")
    keyfile = config.get("ssl", "keyfile")

    ssl = { 'enable': ssl_enable, 'cacert': cacertfile, 
            'cert': certfile, 'key': keyfile } 

    credentials = pika.PlainCredentials(username, password)

    client = ServerRPC(amqp_server, virtualhost, credentials, 
                amqp_exchange, amqp_rkey, ssl)

    client.consume_msg()

if __name__ == "__main__":
    main()
