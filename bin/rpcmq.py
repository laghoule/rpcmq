#!/usr/bin/env python

# Written by Pascal Gauthier <pgauthier@onebigplanet.com>
# 03.09.2012 

import os 
import sys
import uuid
import pika
import ConfigParser
import argparse

__metaclass__ = type

class ClientRPC:
    def __init__(self, amqp_server, rpc_timeout, virtualhost, credentials, 
            amqp_exchange, ssl):
        'Connect to the AMQP bus'

        try:
            if ssl.get('enable') == "on":
                #self.ssl_options = { 'ca_certs': ssl_info.get('cacert'),
                #        'certfile': ssl_info.get('cert'), 'keyfile': ssl_info.get('key') }
                #self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server, 
                #        credentials=credentials, virtual_host=virtualhost, ssl=True, ssl_options=self.ssl_options))
                print "AMQPS support broken right now..."
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server,
                                     credentials=credentials, virtual_host=virtualhost))
            elif ssl.get('enable') == "off":
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server,
                                     credentials=credentials, virtual_host=virtualhost))
        except Exception, err:
            print "AMQP broker exception: %s" % (err)
            self.__on_close__()
            sys.exit(1)

        self.connection.add_timeout(rpc_timeout, self.__on_timeout__)
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.__on_response__, no_ack=True, 
                        queue=self.callback_queue)

    def __on_timeout__(self):
        'Execute on send timeout'

        self.connection.close()
        self.excep_msg = "Consumer timeout (timeout %s)" % (self.timeout)
        raise Exception(self.excep_msg)

    def __on_response__(self, ch, method, props, cmd):
        'Check if reponse correspond to the right ID'

        if self.corr_id == props.correlation_id:
            self.response = cmd

    def __on_close__():
        self.connection.close()

    def produce_msg(self, amqp_server, amqp_exchange, amqp_rkey, amqp_msg):
        'Send AMQ msg'

        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange=amqp_exchange, routing_key=amqp_rkey,
                        properties=pika.BasicProperties(reply_to=self.callback_queue, 
                        correlation_id=self.corr_id,), body=str(amqp_msg))

        while self.response is None:
            self.connection.process_data_events()

        return int(self.response)


def main():
    'Main function'

    parser = argparse.ArgumentParser()
    parser.add_argument("cmd", help="command to be executed remotely")
    parser.add_argument("-c", "--config", dest="config",
            help="path to config FILE", metavar="FILE")

    args = parser.parse_args()

    if not args.config and not args.cmd:
        parser.print_help()
        sys.exit(1)

    config_file = open(args.config, "r")
    config = ConfigParser.RawConfigParser()
    config.readfp(config_file)

    amqp_server = config.get("main", "server")
    rpc_timeout = config.get("main", "rpc_timeout")
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

    client = ClientRPC(amqp_server, int(rpc_timeout), virtualhost,
                credentials, amqp_exchange, ssl)
    response = client.produce_msg(amqp_server, amqp_exchange, 
                amqp_rkey, sys.argv[3])

if __name__ == "__main__":
    main()
