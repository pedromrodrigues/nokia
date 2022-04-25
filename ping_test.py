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
    def __init__(self,network_instance,destination,test_tool):
        Thread.__init__(self)
        self.network_instance = network_instance
        self.destination = destination
        self.test_tool = test_tool
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
                output = os.system(f"ip netns exec {netinst} ping -c 1 {self.destination}")

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

                time.sleep(1)

        return


        #output_2 = subprocess.run(f"ip netns exec {netinst} ping -c 1 {self.destination}", stdout=f)        
        #transform_2 = str(output_2.returncode)
        #logging.info(f'Este é o output {transform_2}')

        #result = str(output.returncode)
        #logging.info(f'>>>>>>>> A cena foi {result}')



##########################################################################
## Proc to process the config notifications received by auto_config_agent
## At present processing config from js_path containing agent_name
##########################################################################
def Handle_Notification(obj, state):
    if obj.HasField('config'):
        logging.info(f"GOT CONFIG :: {obj.config.key.js_path}")

        if obj.config.key.keys:
            ip_fqdn = obj.config.key.keys[0]

        json_str = obj.config.data.json.replace("'", "\"")
        data = json.loads(json_str) if json_str != "" else {}

        # Delete one target ip_fqdn
        if obj.config.op == 2 and obj.config.key.keys:
            logging.info(f"Deleting {ip_fqdn}")
            if state.targets[ ip_fqdn ]['threads']:
                thread.stop = True
                logging.info(f"Joining thread for {ip_fqdn}")
                thread.join()
            del state.targets[ ip_fqdn ]
            Remove_Telemetry( [(f'.ping_test.peer{{.ip=="{ip_fqdn}"}}')] )

        else:
            if 'admin_state' in data:
                state.admin_state = data['admin_state'][12:]
            if 'network_instance' in data:
                state.network_instance = data['network_instance']['value']
            if 'targets' in data:
                test_tool = data['targets']['test_tool'][10:]
                parallel_tests = data['targets']['number_of_parallel_testing']['value']

                if ip_fqdn not in state.targets:
                    state.targets[ ip_fqdn ] = { 'test_tool': test_tool, 'number_of_parallel_testing': parallel_tests, 'threads': [] }
                else:
                    state.targets[ ip_fqdn ].update( { 'test_tool': test_tool, 'number_of_parallel_testing': parallel_tests } )   
            #else:
            #    logging.info(f"Unexpected notification : {obj}")

        if state.admin_state == "enable" and obj.config.op != 2:
            # if a new target was added
            # the monitoring will start for this ip_fqdn
            if obj.config.key.keys:
                for i in range(int(state.targets[ ip_fqdn ]['number_of_parallel_testing'])):
                    new_thread = ServiceMonitoringThread(state.network_instance,ip_fqdn,state.targets[ ip_fqdn ]['test_tool'])
                    state.targets[ ip_fqdn ]['threads'].append(new_thread)
                    logging.info(f" *** Starting a new Thread *** {ip_fqdn}")
                    new_thread.start()
            else:
                for target in state.targets:
                    # if the threads list is empty
                    if not state.targets[ target ]['threads']:
                        for i in range(int(state.targets[ target ]['number_of_parallel_testing'])):
                            new_thread = ServiceMonitoringThread(state.network_instance,target,state.targets[ target ]['test_tool'])
                            state.targets[ target ]['threads'].append(new_thread)
                            logging.info(f" *** Starting a new Thread *** {target}")
                            new_thread.start()

                    # if there are already running threads and test_tool hasn't changed
                    elif len(state.targets[ target ]['threads']) != int(state.targets[ target ]['number_of_parallel_testing']) \
                    and state.targets[ target ]['threads'][0].test_tool == state.targets[ target ]['test_tool']:
                
                        # there is an increase of running threads
                        if len(state.targets[ target ]['threads']) < int(state.targets[ target ]['number_of_parallel_testing']):

                            for j in range(len(state.targets[ target ]['threads']),int(state.targets[ target ]['number_of_parallel_testing'])):
                                new_thread = ServiceMonitoringThread(state.network_instance,target,state.targets[ target ]['test_tool'])
                                state.targets[ target ]['threads'].append(new_thread)
                                new_thread.start()
                        # there is a decrease of running threads
                        else:
                            for j in range(int(state.targets[ target ]['number_of_parallel_testing']),len(state.targets[ target ]['threads'])):
                                thread = state.targets[ target ]['threads'][int(state.targets[ target ]['number_of_parallel_testing'])]
                                thread.stop = True
                                thread.join()
                                state.targets[ target ]['threads'].pop([int(state.targets[ target ]['number_of_parallel_testing'])])
                    # if the number of threads is the same but the test_tool is different
                    else:
                        for thread in state.targets[ target ]['threads']:
                            thread.stop = True
                            thread.join()
                        
                        lock.acquire()
                        count[ target ].update( { 'count': 0 } )
                        lock.release()

                        for j in range(int(state.targets[ target ]['number_of_parallel_testing'])):
                            new_thread = ServiceMonitoringThread(state.network_instance,target,state.targets[ target ]['test_tool'])
                            state.targets[ target ]['threads'].append(new_thread)
                            new_thread.start()

        elif state.admin_state == "disable":
            if state.targets:
                for target in state.targets:
                    for thread in state.targets[ target ]['threads']:
                        thread.stop = True
                        logging.info(f'Joining thread')
                        thread.join()
                    state.targets[ target ]['threads'].clear()
            
        return False

#######################################################################
## State
## Contains the network instance, admin_state and the dict of ip/fqdn
#######################################################################
class State(object):
    def __init__(self):
        self.network_instance = ''
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
                    logging.info('TO DO -commit.end config')
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