## Loni Wood
## February 21, 2023
## Module 7 Assignment


import pika
import sys
import webbrowser
import csv
import time

#####################################################################################

# define variables that will be used throughout
host = "localhost"
route_A_queue = 'route-A'
route_B_queue = 'route-B'
route_C_queue = 'route-C'
data_file = 'traveltime.csv' #faker data
show_offer = True #Define if you want to have the RabbitMQ Admit site opened, True = Y, False = N

######################################################################################

# define option to open rabbitmq admin site
def offer_rabbitmq_admin_site(show_offer):
    if show_offer == True:
    
        """Offer to open the RabbitMQ Admin website"""
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
                webbrowser.open_new("http://localhost:15672/#/queues")
                print()

##########################################################################################

## define delete_queue
def delete_queue(host: str, queue_name: str):
    """
    Delete queues each time we run the program to clear out old messages.
    """
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    ch = conn.channel()
    ch.queue_delete(queue=queue_name)

############################################################################################

## define send message to the queue
def send_message_to_queue(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.
    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a  server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message} on {queue_name}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()
    
########################################################################################
    
# defining reading in file

def get_data_from_csv(file):
   
    #Define main body function
    #read from a file to get some fake data

    file =  open(data_file,"r")
    # create a csv reader for our comma delimited data
    reader = csv.reader(file, delimiter=",")
    
    # skipping header row
    next(reader)
    

    for row in reader:
        fstring_time_column = f"{row[0]}"
    
    # since of of the columns are numbers, converting them to floats
    # convert to float       
        try:
            route_a_column = float(row[1])
            fstring_route_a_column1 = f"[{fstring_time_column}, {route_a_column}]"
            route_a_message = fstring_route_a_column1.encode()
            send_message_to_queue(host, route_A_queue, route_a_message)
        except ValueError:
            pass
        
       
        try:
            route_b_column = float(row[2])
            fstring_route_b_column2 = f"[{fstring_time_column}, {route_b_column}]"
            route_b_message = fstring_route_b_column2.encode()
            send_message_to_queue(host, route_B_queue, route_b_message)
        except ValueError:
            pass
          
        
        try:
            route_c_column = float(row[3])
            fstring_route_c_column3 = f"[{fstring_time_column}, {route_c_column}]"
            route_c_message = fstring_route_c_column3.encode()
            send_message_to_queue(host, route_C_queue, route_c_message)
        except ValueError:
           pass
       
        
    # sleep in seconds
        time.sleep(1)

###########################################################################################
            
# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    # setting show_offer for RabbitMQ site to off
    offer_rabbitmq_admin_site(show_offer)
   
    # delete queues to clear old messages
    delete_queue(host, route_A_queue)
    delete_queue(host, route_B_queue)
    delete_queue(host, route_C_queue)
    
    get_data_from_csv(data_file)
    