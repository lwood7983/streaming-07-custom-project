"""
    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

### Name:  Loni Wood
### Date:  February 21, 2023
"""

import pika
import sys
import time
from collections import deque
#####################################################################################

# define variables that will be used throughout
host = "localhost"
route_C_queue = 'route-C'


#######################################################################################
# defining routeC deque
""" 
For the Route C travel time, we want to know if the travel time increases by 15 mins 
or more in a 12 minute window.

At one reading every 1/2 minute, the Route C time deque max length is 24 (12 min * 1 reading/0.5 min)
"""
route_C_time_deque = deque(maxlen=24) # limited to 24 items (the 24 most recent readings)


# set alert limits

route_C_alert_limit = 15 # if the Route C travel time increases by more than 15 minutes then send alert (RouteC Slowed)


#######################################################################################
## define delete_queue
def delete_queue(host: str, queue_name: str):
    """
    Delete queues each time we run the program to clear out old messages.
    """
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    ch = conn.channel()
    ch.queue_delete(queue=queue_name)
########################################################################################
# define a callback function to be called when a message is received
# defining callback for routeC queue

def route_C_callback(ch, method, properties, body):
    """ Define behavior on getting a message about the Route C travel time.  Since the producer 
    ignored the blanks in the code, the consumer will not be receiving any of the blank rows."""
    # decode the binary message body to a string
    message = body.decode()
    print(f" [x] Received {message} on route-C")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    # sleep in seconds to watch the alerts
    time.sleep(.5)
    # def routeC deque queue
    # adding message to the route C deque
    route_C_time_deque.append(message)

    # identifying first item in the deque
    route_C_deque = route_C_time_deque[0]
    # splitting date & timestamp from column
    # will now have date & timestamp in index 0 and travel time in index 1
    # this will be looking at what occurred 12 mins prior (24 messages ago)
    route_C_deque_split = route_C_deque.split(",")
    # converting temp in index 1 to float and removing last character  
    route_C_time1 = float(route_C_deque_split[1][:-1])
   
    # defining current RouteC travel time
    route_C_curr_time = message
    # splitting date & timestamp from temp in column
    # will now have date & timestamp in index 0 and temp in index 1
    # this will be looking at what occurred 12 mins prior (24 messages ago)
    route_C_curr_column = route_C_curr_time.split(",")     
    # converting temp in index 1 to float and removing last character    
    route_C_now_time = float(route_C_curr_column[1][:-1])
    
    # defining route C travel time change and calculating the difference
    
    route_C_travel_change = (route_C_now_time - route_C_time1)
        # defining routeC alert
    if route_C_travel_change >= route_C_alert_limit:
        print(f" Route C Slowed!! The travel time for Route C has increased by 15 minutes or more in 12 min (or 24 readings). \n          Route_C_Travel_Time_Increase = {route_C_travel_change} minutes = {route_C_now_time} - {route_C_time1}")
       

# define a main function to run the program
def main(hn: str, qn: str):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=route_C_queue, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume( queue=route_C_queue, on_message_callback=route_C_callback)
        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main(host, route_C_queue)