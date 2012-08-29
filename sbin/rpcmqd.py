#!/usr/bin/env python

# Written by Pascal Gauthier <pgauthier@onebigplanet.com>
# 03.09.2012 

import pwd
import daemon
import atexit
import fcntl
import socket
import time
import sys
import os
import subprocess
import pika
import syslog
import ConfigParser
import argparse

__metaclass__ = type

class PidFile(object):
    """Context manager that locks a pid file.  Implemented as class
    not generator because daemon.py is calling .__exit__() with no parameters
    instead of the None, None, None specified by PEP-343.

    From: http://code.activestate.com/recipes/577911-context-manager-for-a-daemon-pid-file/"""
    # pylint: disable=R0903

    def __init__(self, path):
        self.path = path
        self.pidfile = None

    def __enter__(self):
        self.pidfile = open(self.path, "a+")
        try:
            fcntl.flock(self.pidfile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            raise SystemExit("Already running according to " + self.path)
        self.pidfile.seek(0)
        self.pidfile.truncate()
        self.pidfile.write(str(os.getpid()))
        self.pidfile.flush()
        self.pidfile.seek(0)
        return self.pidfile

    def __exit__(self, exc_type=None, exc_value=None, exc_tb=None):
        try:
            self.pidfile.close()
        except IOError as err:
            # ok if file was just closed elsewhere
            if err.errno != 9:
                raise
        os.remove(self.path)


class ServerRPC:
    def __init__(self, amqp_server, virtualhost, credentials, amqp_exchange, amqp_rkey, ssl_info):
        "Connect to AMQP bus and request queue, and bind to exchange"

        # Vars
        self.rpc_cmdpath = "/opt/rpc-scripts/bin/"

        while True:
            try:
                if ssl_info.get('enable') == "on":
                    #self.ssl_options = { 'ca_certs': ssl_info.get('cacert'), 
                    #                    'certfile': ssl_info.get('cert'), 'keyfile': ssl_info.get('key') }
                    #self.amqp_connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server,
                    #                            credentials=credentials, virtual_host=virtualhost, ssl=True,
                    #                            ssl_options=self.ssl_options)) 
                    print "AMQPS support broken right now..."
                    self.amqp_connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server,
                                            credentials=credentials, virtual_host=virtualhost))
                elif ssl_info.get('enable') == "off":
                    self.amqp_connection = pika.BlockingConnection(pika.ConnectionParameters(host=amqp_server,
                                            credentials=credentials, virtual_host=virtualhost))
                else:
                    raise Exception("ssl enable = on/off")
                self.amqp_channel = self.amqp_connection.channel()
            except socket.error, err:
                print """Connection error (%s), will try again 
                        in 5 sec...""" % (err)
                time.sleep(5)
            except Exception, err:
                print "Error (%s), will try again in 5 sec..." % (err)
                self.amqp_connection.close()
            else:
                break

        self.result = self.amqp_channel.queue_declare(exclusive=True)
        self.amqp_queue = self.result.method.queue

        self.amqp_channel.queue_bind(exchange=amqp_exchange, queue=self.amqp_queue,
                       routing_key=amqp_rkey)

    def __execute_cmd__(self, response_channel, method, properties, cmd):
        "Execute command in /opt/rpc-scripts path"

        # Log file for the rpc-scripts
        rpc_cmdlog = open("/opt/rpc-scripts/log/" + cmd + ".log", "a+", 0)

        # Subprocess to execute
        self.response = subprocess.call(self.rpc_cmdpath + cmd, 
                            stdout=rpc_cmdlog,
                            stderr=rpc_cmdlog,
                            shell=True)

        # Trace to syslog
        self.syslog_msg = ("%s: return status code %s") % (self.rpc_cmdpath + cmd, self.response)
        syslog.openlog("rpcmqd")
        syslog.syslog(self.syslog_msg)

        # Close the rpc_cmdlog
        rpc_cmdlog.close()

        # Send a reponse to the client
        response_channel.basic_publish(exchange='', routing_key=properties.reply_to, 
                            properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                            body=str(self.response))

        response_channel.basic_ack(delivery_tag = method.delivery_tag)

        return self.response

    def consume_msg(self):
        "Consume AMQP message"

        self.amqp_channel.basic_qos(prefetch_count=1)
        self.amqp_channel.basic_consume(self.__execute_cmd__, queue=self.amqp_queue)
        try:
            self.amqp_channel.start_consuming()
        except KeyboardInterrupt, err: 
            print "Keyboard interruption"

    def close(self):
        "Close all connection"

        self.amqp_connection.close()


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

    # Uniq name for pid and log, based on config name
    rpcmqd_instance = args.config.split("/")[-1].split(".conf")[0]

    # Config vars
    amqp_server = config.get("main", "amqp_server")
    run_as_uid = config.get("main", "run_as_uid")
    run_with_umask = config.get("main", "run_with_umask")
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

    # Daemonification
    stdout_file = open("/opt/rpc-scripts/log/" + rpcmqd_instance + ".log", "a+", 0)
    context = daemon.DaemonContext(
                working_directory="/opt/rpc-scripts/",
                umask=int(run_with_umask),
                uid=pwd.getpwnam(run_as_uid).pw_uid,
                gid=pwd.getpwnam(run_as_uid).pw_gid,
                detach_process=False, # For debug purpose
                stdout=stdout_file,
                stderr=stdout_file,
                pidfile=PidFile("run/" + rpcmqd_instance + ".pid")
                )

    with context:
        client = ServerRPC(amqp_server, virtualhost, credentials, 
                    amqp_exchange, amqp_rkey, ssl)

        # Run when exiting
        atexit.register(client.close)

        # Consume
        client.consume_msg()


if __name__ == "__main__":
    main()
