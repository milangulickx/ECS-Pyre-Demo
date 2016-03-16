################################################################################################
#   Pyre is a python implementation of the ZRE protocol: http://rfc.zeromq.org/spec:36
#   Discovery problem: http://hintjens.com/blog:32
#
#   - ZRE protocol used to discover local peers:
#       - Every node listens on UDP port 5670
#       - Every node broadcasts at regular intervals on UDP port 5670, to identify himself
#   - Once peer discovered, TCP connection with EACH peer separately (using ZeroMQ) is set up to send messages
#
#   ==> No central server required!
#
#
#   Copyright 2016 Maxime Bossens
#   Bitbucket: https://mbossens@bitbucket.org/mbossens/ecs-pyre-demo.git
#
################################################################################################

from pyre import Pyre
from pyre import zhelper 
import zmq 
import uuid
import logging
import sys
import json

############## CAR SPECIFICATIONS ##############

MANUFACTURER = "TESLA"
MODEL = "MODEL_X"

################################################

GROUPNAME = "TESLA_NET"
STOP_COMMAND = "$$CAR_STOP"

def chat_task(ctx, pipe):
    print("-----CAR PEER COMMUNICATION STARTED-----")
    print("Manufacturer: ", MANUFACTURER, " - Model: ", MODEL)

    connected_cars = 0

    #Set up node for the car
    n = Pyre("")
    n.set_header("manufacturer", MANUFACTURER)
    n.set_header("model", MODEL)

    #Join the group 'chat'
    n.join(GROUPNAME)

    #Start broadcasting node
    n.start()

    # Set up poller
    poller = zmq.Poller()
    poller.register(pipe, zmq.POLLIN)  #Local pipe (contains commands/messages we send through terminal)
    poller.register(n.socket(), zmq.POLLIN)


    # A while loop constantly polls for new items = PULL system
    while True:

        #Wait for new message to be polled. This function blocks until there is a new message
        items = dict(poller.poll())

        #This are messages from ourselves, that we want to shout on the network
        if pipe in items:
            message = pipe.recv()

            # User stopped car
            if message.decode('utf-8') == STOP_COMMAND:
                break

            print(">>>>>> Sending out shout: %s" % message)
            n.shouts(GROUPNAME, message.decode('utf-8'))

        # Received messages from system or messages from other peers
        else:
            cmds = n.recv()
            print("--------------------------------------------------------------------------------")
            #print(">>>>>>>RECEIVED MESSAGE: ", cmds)

            msg_type = cmds.pop(0)
            car_uuid = uuid.UUID(bytes=cmds.pop(0))
            msg_name = cmds.pop(0)

            if msg_type.decode('utf-8') == "ENTER":
                headers = json.loads(cmds.pop(0).decode('utf-8'))
                print(">>>> NEW CAR DISCOVERED IN NETWORK")
                print("---Manufacturer:", headers.get("manufacturer"), "--- Model:", headers.get("model"))

            elif msg_type.decode('utf-8') == "JOIN":
                print(">>>> NEW CAR JOINED GROUP <<", cmds.pop(0).decode('utf-8'),">>")
                connected_cars += 1

            elif msg_type.decode('utf-8') == "SHOUT":
                print(">>>> RECEIVED SHOUT IN %s" % cmds.pop(0))
                print("---Msg: %s" % cmds.pop(0))

            elif msg_type.decode('utf-8') == "EXIT":
                print(">>>> CAR LEFT NETWORK")
                connected_cars -= 1


            print("---Total connected cars: ", connected_cars)
            print("---Car_UUID: ", car_uuid)
            #print("---NODE_MSG REMAINING: %s" % cmds)


    print("-----CAR COMMUNICATION STOPPED-----")
    n.stop()

if __name__ == '__main__':

    # For logging
    logger = logging.getLogger("pyre")
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logger.propagate = False

    #Create ZMQ context
    ctx = zmq.Context()

    # Set up a background chat_task thread. We use ZeroMQ to send inter-thread messages to that thread
    local_car_pipe = zhelper.zthread_fork(ctx, chat_task)

    # For python 2 versions, text input of user is differently defined
    input = input
    if sys.version_info.major < 3:
        input = raw_input

    while True:
        try:
            msg = input()
            local_car_pipe.send(msg.encode('utf_8')) #Send the input message to the local car pipe
        except (KeyboardInterrupt, SystemExit):
            break

    print("--------------------------------------------------------------------------------")
    local_car_pipe.send(STOP_COMMAND.encode('utf_8'))
    print("LEAVING NETWORK")