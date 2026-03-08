'''
Grabs the highest priority ticket (lowest score) from the queue
and fetch details from the database
'''

import redis
import time

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_USER = 'default'
REDIS_PASSWORD = 'holtech_torch777'
QUEUE_NAME = "support_queue"

# Connect to Redis
r = redis.Redis(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    db=REDIS_DB,
    username=REDIS_USER,
    password=REDIS_PASSWORD, 
    decode_responses=True
)

print("--- Support Agent Online. Waiting for tickets ---")

while True:
    # 1. Get the highest priority item (lowest score) from the Queue
    # zpopmin returns a list of tuples: [('TICK-101', 1.0)]
    result = r.zpopmin(QUEUE_NAME)
    
    if result:
        ticket_id, score = result[0]
        
        # 2. Fetch the REAL data from the Database (Hash)
        key_name = f"ticket:{ticket_id}"
        ticket_data = r.hgetall(key_name)
        
        # 3. Process the ticket
        print("\n" + "="*40)
        print(f"NEW TASK ASSIGNED (Priority Score: {int(score)})")
        print("-" * 40)
        print(f"  Ticket ID:   {ticket_data.get('ticket_id')}")
        print(f"  Customer:    {ticket_data.get('customer')}")
        print(f"  Severity:    {ticket_data.get('severity')}")
        print(f"  Issue:       {ticket_data.get('issue')}")
        print("="*40 + "\n")
        
        # Simulate working on the ticket
        print("Agent is resolving the issue...")
        time.sleep(2)
        print(f"Ticket {ticket_id} resolved.\n")
        
    else:
        # Queue is empty
        print("Queue is empty. Waiting for new tickets...")
        time.sleep(3)
        break # Exit for the demo purpose
