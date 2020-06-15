import zmq
import time
import json

port = str(5555)
context = zmq.Context()

socket = context.socket(zmq.PAIR)

socket.bind("tcp://*:%s" % port)

while True:
    print('Job executor is waiting for new job request')
    msg = socket.recv()
    print('Job executor got mess')
    msg_dict = json.loads(msg.decode('ascii'))
    print(msg_dict)
    msg_dict_value = msg_dict['job_ID']
    print(msg_dict_value)
    print('Job executor is starting')
    time.sleep(3)
    print('Job completed')
    print('Job executor is about to report the completion of the hob back to the requester')
    msg_content = {}
    msg_content['some other key'] = msg_dict_value
    msg_content_str = str(json.dumps(msg_content))
    msg_content_str = msg_content_str.encode('ascii')
    socket.send(msg_content_str)

