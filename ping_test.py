#!/usr/bin/env python
# coding=utf-8

import grpc

from datetime import datetime
import time
import sys
import logging
import os
import json
import socket
import subprocess
import threading
import traceback

import sdk_service_pb2
import sdk_service_pb2_grpc
import config_service_pb2
import sdk_common_pb2

# To report state back
import telemetry_service_pb2
import telemetry_service_pb2_grpc

from logging.handlers import RotatingFileHandler

import netns
import signal

##############
## Agent name
##############
agent_name='ping_test'

########################################################
## Open a GRPC channel to connect to sdk_mgr on the dut
## sdk_mgr will be listening on 50053
########################################################
channel = grpc.insecure_channel('127.0.0.1:50053')
metadata = [('agent_name', agent_name)]
stub = sdk_service_pb2_grpc.SdkMgrServiceStub(channel)
lock = threading.Lock()
count = {}

from enum import Enum
class Threads_Check(Enum):
    LOWER = 1
    GREATER = 2
    EVEN = 3

class Existence(Enum):
    NEW = 1
    ALREADY_EXISTED = 2

###############################
## Subscribe to required event
###############################
def Subscribe(stream_id, option):

    op = sdk_service_pb2.NotificationRegisterRequest.AddSubscription

    if option == 'cfg':
        entry = config_service_pb2.ConfigSubscriptionRequest()
        request = sdk_service_pb2.NotificationRegisterRequest(op=op, stream_id=stream_id, config=entry)

    subscription_response = stub.NotificationRegister(request=request, metadata=metadata)
    logging.info( f'Status of subscription response for {option}:: {subscription_response.status}' )

###############################################
## Subscribe to all the events that Agent needs
###############################################
def Subscribe_Notifications(stream_id):

    if not stream_id:
        logging.info("Stream ID not sent.")
        return False
    
    Subscribe(stream_id, 'cfg')

def Add_Telemetry(path_obj_list):
    telemetry_stub = telemetry_service_pb2_grpc.SdkMgrTelemetryServiceStub(channel)
    telemetry_update_request = telemetry_service_pb2.TelemetryUpdateRequest()
    for js_path, obj in path_obj_list:
        telemetry_info = telemetry_update_request.state.add()
        telemetry_info.key.js_path = js_path
        telemetry_info.data.json_content = json.dumps(obj)
    logging.info(f"Telemetry_update_request :: {telemetry_update_request}")
    telemetry_response = telemetry_stub.TelemetryAddOrUpdate(request=telemetry_update_request, metadata=metadata)
    return telemetry_response

def Remove_Telemetry(js_paths):
    telemetry_stub = telemetry_service_pb2_grpc.SdkMgrTelemetryServiceStub(channel)
    telemetry_del_request = telemetry_service_pb2.TelemetryDeleteRequest()
    for path in js_paths:
        telemetry_key = telemetry_del_request.key.add()
        telemetry_key.js_path = path
    logging.info(f"Telemetry_Delete_Request :: {telemetry_del_request}")
    telemetry_response = telemetry_stub.TelemetryDelete(request=telemetry_del_request, metadata=metadata)
    return telemetry_response


from threading import Thread
class ServiceMonitoringThread(Thread):
    def __init__(self,network_instance,destination,test_tool,source_ip):
        Thread.__init__(self)
        self.network_instance = network_instance
        self.destination = destination
        self.test_tool = test_tool
        self.source_ip = source_ip
        self.stop = False
        self.in_count = False
        self.state_per_service = {}

    def run(self):

        global count

        netinst = f"srbase-{self.network_instance}"
        while not os.path.exists(f'/var/run/netns/{netinst}'):
            logging.info(f"Waiting for {netinst} netns to be created...")
            time.sleep(1)

        peer = self.destination

        if not self.in_count:
            lock.acquire()
            if peer not in count:
                count[ peer ] = { 'count': 0 }
                self.in_count = True
            lock.release()

        while not self.stop:
            
            if self.test_tool == 'ping':

                #cmd = f"ip netns exec {netinst} ping -c 1 {self.destination}"
                output = os.system(f"ip netns exec {netinst} ping -c 1 {self.destination}")
                #output = subprocess.run(cmd, shell=True)

                lock.acquire()
                count[ peer ].update( { 'count': count[ peer ]['count']+1 } )
                lock.release()

                now_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

                if output == 0:
                    service_status = True
                    logging.info(f"Service available on {self.destination} !")

                else:
                    service_status = False
                    logging.info(f"Service unavailable on {self.destination} !")

                lock.acquire()
                data = {
                        'last_update': { "value" : now_ts },
                        'tests-performed': count[ peer ]['count'],
                        'status-up': service_status
                }
                Add_Telemetry( [(f'.ping_test.peer{{.ip=="{peer}"}}', data )] )
                lock.release()

                time.sleep(10)
            
            if self.test_tool == "httping":

                logging.info("HTTTTTTTP 2.0")
                time.sleep(20)

        return

##########################################################################
## Function that stops the parallel tests (threads)
## It can be performed for all IP FQDN if admin_state=disable
## Or just for a single IP FQDN depending on the flag option 
##########################################################################
def Stop_Target_Threads(state,option,ip_fqdn=None):
    if option == "all":
        for ip_fqdn in state.targets:
            if state.targets[ ip_fqdn ]['admin_state'] == "enable":
                for thread in state.targets[ ip_fqdn ]['threads']:
                    thread.stop = True
                    logging.info(f'Joining thread')
                    thread.join()
                state.targets[ ip_fqdn ]['threads'].clear()
        return

    elif option == "single":
        try:   
            for thread in state.targets[ ip_fqdn ]['threads']:
                thread.stop = True
                logging.info(f"Joining thread for {ip_fqdn}")
                thread.join()
            state.targets[ ip_fqdn ]['threads'].clear()
        except TypeError:
            logging.info(f"Error on Joining Thread :: Stop_Target_Threads()")
        else:
            return

##########################################################################
## Function that starts the parallel tests (threads)
## It can be performed for all admin_state=enable IP FQDN
## Or just for a single IP FQDN depending on the flag option 
##########################################################################
def Start_Target_Threads(state,option,ip_fqdn=None):
    if option == "all":
        for ip_fqdn in state.targets:                
            if state.targets[ ip_fqdn ]['admin_state'] == "enable":
                for i in range(int(state.targets[ ip_fqdn ]['number_of_parallel_tests'])):
                    new_thread = ServiceMonitoringThread(
                        state.targets[ ip_fqdn ]['network_instance'],
                        ip_fqdn,
                        state.targets[ ip_fqdn ]['test_tool'],
                        state.targets[ ip_fqdn ]['source_ip']
                    )
                    state.targets[ ip_fqdn ]['threads'].append(new_thread)
                    logging.info(f" *** Starting a new Thread *** {ip_fqdn}")
                    new_thread.start()
        return
    
    if option == "single":
        try:
            for i in range(int(state.targets[ ip_fqdn ]['number_of_parallel_tests'])):
                new_thread = ServiceMonitoringThread(
                    state.targets[ ip_fqdn ]['network_instance'],
                    ip_fqdn,
                    state.targets[ ip_fqdn ]['test_tool'],
                    state.targets[ ip_fqdn ]['source_ip']
                )
                state.targets[ ip_fqdn ]['threads'].append(new_thread)
                logging.info(f" *** Starting a new Thread *** {ip_fqdn}")
                new_thread.start()
        except TypeError:
            logging.info("Error creating Thread :: Start_Target_Threads()")
        else:
            return

##########################################################################
## Function that checks if the number of parallel tests were changed 
##########################################################################
def Check_Number_Tests(state,ip_fqdn):
    if int(state.targets[ ip_fqdn ]['number_of_parallel_tests']) > len(state.targets[ ip_fqdn ]['threads']):
        return Threads_Check.GREATER.name
    elif int(state.targets[ ip_fqdn ]['number_of_parallel_tests']) < len(state.targets[ ip_fqdn ]['threads']):
        return Threads_Check.LOWER.name
    else:
        return Threads_Check.EVEN.name

#############################################################################
## Function that checks if changes on source_ip or test_tool were performed 
#############################################################################
def Source_IP_or_Tool_Change(state,ip_fqdn):
    if state.targets[ ip_fqdn ]['source_ip'] != state.targets[ ip_fqdn ]['threads'][0].source_ip or \
    state.targets[ ip_fqdn ]['test_tool'] != state.targets[ ip_fqdn ]['threads'][0].test_tool:
        return True
    else:
        return False

##########################################################################
## Function that creates or updates the target IP FQDN information 
##########################################################################
def Update_Target(state,data,ip_fqdn):

    admin_state = data['admin_state'][12:]
    network_instance = data['network_instance']['value']
    test_tool = data['test_tool'][10:]
    parallel_tests = data['number_of_parallel_tests']['value']
    source_ip = data['source_ip']['value']

    if ip_fqdn not in state.targets:
        state.targets[ ip_fqdn ] = { 
            'admin_state': admin_state,
            'network_instance': network_instance,
            'test_tool': test_tool,
            'number_of_parallel_tests': parallel_tests,
            'source_ip': source_ip,
            'threads': []
            }
        return Existence.NEW.name

    else:
        state.targets[ ip_fqdn ].update( { 
            'admin_state': admin_state,
            'network_instance': network_instance,
            'test_tool': test_tool,
            'number_of_parallel_tests': parallel_tests,
            'source_ip':source_ip
            } )
        return Existence.ALREADY_EXISTED.name

##########################################################################
## Proc to process the config notifications received by auto_config_agent
## At present processing config from js_path containing agent_name
##########################################################################
def Handle_Notification(obj, state):

    if obj.HasField('config'):
        logging.info(f"GOT CONFIG :: {obj.config.key.js_path}")

        json_str = obj.config.data.json.replace("'", "\"")
        data = json.loads(json_str) if json_str != "" else {}

        if obj.config.key.keys:
            ip_fqdn = obj.config.key.keys[0]

        if obj.config.op == 2 and obj.config.key.keys:
            if state.targets[ ip_fqdn ]['threads']:
                Stop_Target_Threads(state,"single",ip_fqdn)
            del state.targets[ ip_fqdn ]
            lock.acquire()
            del count[ ip_fqdn ]
            lock.release()
            Remove_Telemetry( [(f'.ping_test.peer{{.ip=="{ip_fqdn}"}}')] )

        elif 'admin_state' in data:
            state.admin_state = data['admin_state'][12:]

            if state.admin_state == "enable":
                Start_Target_Threads(state,"all")
            else:
                Stop_Target_Threads(state,"all")
  
        elif 'targets' in data:

            result = Update_Target(state, data['targets'], ip_fqdn)

            if state.targets[ ip_fqdn ]['admin_state'] == "enable" and state.admin_state == "enable":
                if result == "NEW":
                    Start_Target_Threads(state,"single",ip_fqdn)

                if result == "ALREADY_EXISTED":
                    if not state.targets[ ip_fqdn ]['threads']:
                        Start_Target_Threads(state,"single",ip_fqdn)

                    else:
                        if Source_IP_or_Tool_Change(state,ip_fqdn):
                            Stop_Target_Threads(state,"single",ip_fqdn)
                            lock.acquire()
                            count[ ip_fqdn ].update( { 'count': 0 } )
                            lock.release()
                            Start_Target_Threads(state,"single",ip_fqdn)
                        else:
                            testsChange = Check_Number_Tests(state,ip_fqdn)
                            if testsChange == "LOWER":
                                while int(state.targets[ ip_fqdn ]['number_of_parallel_tests']) != len(state.targets[ ip_fqdn ]['threads']):
                                    thread = state.targets[ ip_fqdn ]['threads'][len(state.targets[ ip_fqdn ]['threads'])-1]
                                    thread.stop = True
                                    logging.info(f"Joining thread for {ip_fqdn}")
                                    thread.join()
                                    state.targets[ ip_fqdn ]['threads'].remove(thread)
                                
                            elif testsChange == "GREATER":
                                for j in range(len(state.targets[ ip_fqdn ]['threads']),int(state.targets[ ip_fqdn ]['number_of_parallel_tests'])):
                                    new_thread = ServiceMonitoringThread(
                                        state.targets[ ip_fqdn ]['network_instance'],
                                        ip_fqdn,
                                        state.targets[ ip_fqdn ]['test_tool'],
                                        state.targets[ ip_fqdn ]['source_ip']
                                    )
                                    state.targets[ ip_fqdn ]['threads'].append(new_thread)
                                    logging.info(f" *** Starting a new Thread *** {ip_fqdn}")
                                    new_thread.start()

            if state.targets[ ip_fqdn ]['admin_state'] == "disable" and state.admin_state == "enable":
                Stop_Target_Threads(state,"single",ip_fqdn)
 
    else:
        logging.info(f"Unexpected notification : {obj}")

    return False

#######################################################################
## State
## Contains the admin_state and the dict of each target ip/fqdn
#######################################################################
class State(object):
    def __init__(self):
        self.admin_state = ''
        self.targets = {}
    
    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

##################################################################################################
## This is the main kproc where all processing for ping_test starts.
## Agent registeration, notification registration, Subscrition to notifications.
## Waits on the sunscribed Notifications and once any config is received, handles that config
## If there are critical errors, Unregisters the ping_test gracefully.
##################################################################################################
def Run():

    sub_stub = sdk_service_pb2_grpc.SdkNotificationServiceStub(channel)

    response = stub.AgentRegister(request=sdk_service_pb2.AgentRegistrationRequest(), metadata=metadata)
    logging.info(f"Registration response: {response.status}")

    request = sdk_service_pb2.NotificationRegisterRequest(op=sdk_service_pb2.NotificationRegisterRequest.Create)
    create_subscription_response = stub.NotificationRegister(request=request, metadata=metadata)
    stream_id = create_subscription_response.stream_id
    logging.info(f"Create subscription response received. stream_id: {stream_id}")

    try:
        Subscribe_Notifications(stream_id)

        stream_request = sdk_service_pb2.NotificationStreamRequest(stream_id=stream_id)
        stream_response = sub_stub.NotificationStream(stream_request, metadata=metadata)

        state = State()
        count = 1

        for r in stream_response:
            logging.info(f"Count :: {count} NOTIFICATION:: \n{r.notification}")
            count += 1

            for obj in r.notification:

                if obj.HasField('config') and obj.config.key.js_path == '.commit.end':
                    logging.info("TO DO -.commit.end")
                else:
                    Handle_Notification(obj, state)
                    logging.info(f'Updated state: {state}')
   
    finally:
        Exit_Gracefully(0,0)

    return True


##########################################################
## Gracefully handles SIGTERM signal
## When called, will unregister agent and gracefully exit
##########################################################
def Exit_Gracefully(signum, frame):
    logging.info( f"Caught signal :: {signum}\n will unregister Ping Test" )

    main_thread = threading.current_thread()

    for thread in threading.enumerate():
        if thread is main_thread:
            continue
        thread.stop = True
        logging.info(f"Thread joining...")
        thread.join()
        
    try:
        response = stub.AgentUnRegister(request=sdk_service_pb2.AgentRegistrationRequest(), metadata=metadata)
        logging.info( f'Exit_Gracefully: Unregister response:: {response}' )
    finally:
        logging.info( f'GOING TO EXIT NOW' )
        sys.exit()

#################################
## Main from where the Agent starts
## Log file is written
## Signals handled
#################################
if __name__ == '__main__':

    hostname = socket.gethostname()
    stdout_dir = '/var/log/srlinux/stdout' # PyTEnv.SRL_STDOUT_DIR
    signal.signal(signal.SIGTERM, Exit_Gracefully)
    if not os.path.exists(stdout_dir):
        os.makedirs(stdout_dir, exist_ok=True)
    log_filename = f'{stdout_dir}/{agent_name}.log'
    logging.basicConfig(filename=log_filename, filemode='a', \
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',\
                        datefmt='%H:%M:%S', level=logging.INFO)
    handler = RotatingFileHandler(log_filename, maxBytes=3000000, backupCount=5)
    logging.getLogger().addHandler(handler)
    logging.info("START TIME :: {}".format(datetime.now()))
    if Run():
        logging.info('Agent unregistered')
    else:
        logging.info(f'Some exception caught, Check!')