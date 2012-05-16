#!/usr/bin/env python

# Written by Pascal Gauthier <pgauthier@onebigplanet.com>
# 03.09.2012 

import os 
import sys
import uuid
import pika
import ConfigParser

usage = "Usage: rpcmqd.py -c config_file cmd"

__metaclass__ = type

def read_config(config_file, section, var):
    'Read config and return value'

    config = ConfigParser.RawConfigParser()
    config.read(config_file)

    value = config.get(section, var)

    return value


class ClientRPC:
    def __init__(self, amqp_server, rpc_timeout, virtualhost, credentials, amqp_exchange, ssl):
        'Connect to the AMQP bus'

        try:
            if ssl.get('enable') == "on":
                #self.ssl_options = { 'ca_certs': ssl_info.get('cacert'), 'certfile': ssl_info.get('cert'), 'keyfile': ssl_info.get('key') }
                #self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server, credentials=credentials, virtual_host=virtualhost, ssl=True, ssl_options=self.ssl_options))
                print "AMQPS support broken right now (blame pika)... fallback to normal AMQP"
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server, credentials=credentials, virtual_host=virtualhost))
            elif ssl.get('enable') == "off":
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server, credentials=credentials, virtual_host=virtualhost))
        except Exception, err:
            print "Exception: %s" % (err)

        self.connection.add_timeout(rpc_timeout, self.__on_timeout__)
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.__on_response__, no_ack=True, queue=self.callback_queue)

    def __on_timeout__(self):
        'Execute on send timeout'

        self.connection.close()
        self.excep_msg = "Consumer did not respond in time (timeout %s)" % (self.timeout)
        raise Exception(self.excep_msg)

    def __on_response__(self, ch, method, props, cmd):
        'Check if reponse correspond to the right ID'

        if self.corr_id == props.correlation_id:
            self.response = cmd

    def produce_msg(self, amqp_server, amqp_exchange, amqp_rkey, amqp_msg):
        'Send AMQ msg'

        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange=amqp_exchange, routing_key=amqp_rkey, properties=pika.BasicProperties(reply_to=self.callback_queue, correlation_id=self.corr_id,), body=str(amqp_msg))

        while self.response is None:
            self.connection.process_data_events()

        return int(self.response)


def main():
    'Main function'

    if len(sys.argv) == 4 and sys.argv[1]:
        if os.path.exists(sys.argv[2]):
            config_file = sys.argv[2]
            amqp_server = read_config(config_file, "main", "server")
            rpc_timeout = read_config(config_file, "main", "rpc_timeout")
            amqp_exchange = read_config(config_file, "rpc-context", "exchange")
            amqp_rkey = read_config(config_file, "rpc-context", "routing_key")
            virtualhost = read_config(config_file, "rpc-context", "virtualhost")
            username = read_config(config_file, "rpc-context", "username")
            password = read_config(config_file, "rpc-context", "password")
            ssl_enable = read_config(config_file, "ssl", "enable")
            cacertfile = read_config(config_file, "ssl", "cacertfile")
            certfile = read_config(config_file, "ssl", "certfile")
            keyfile = read_config(config_file, "ssl", "keyfile")
            ssl = { 'enable': ssl_enable, 'cacert': cacertfile, 'cert': certfile, 'key': keyfile }
            credentials = pika.PlainCredentials(username, password)
        else:
            err_msg = "File %s don't exist" % (sys.argv[2])
            raise IOError(err_msg) 
        client = ClientRPC(amqp_server, int(rpc_timeout), virtualhost, credentials, amqp_exchange, ssl)
        response = client.produce_msg(amqp_server, amqp_exchange, amqp_rkey, sys.argv[3])
    else:
        raise ValueError(usage)

    return int(response)

main()
