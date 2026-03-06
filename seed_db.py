'''
Script to populate data in Redis
'''

import redis
import json
import os

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_USER = 'default'
REDIS_PASSWORD = 'holtech_torch777'
QUEUE_NAME = "support_queue"

# Connect to Redis
try:
    r = redis.Redis(
        host=REDIS_HOST, 
        port=REDIS_PORT,
        username=REDIS_USER,
        password=REDIS_PASSWORD, 
        decode_responses=True
    )

    # Test connection immediately
    print("Testing connection...")
    print(r.ping()) 
    print("Connection successful!")

except Exception as e:
    print(f"Connection failed: {e}")
    exit()

# Clear old data for a clean simulation
print("Clearing old data...")
r.delete(QUEUE_NAME)

# We delete specific keys to avoid dropping everything if DB is shared
for key in r.scan_iter("ticket:*"):
    r.delete(key)

# Load data from JSON file
file_path = 'data/tickets.json'
if not os.path.exists(file_path):
    print(f"Error: {file_path} not found.")
    exit()

with open(file_path, 'r') as f:
    tickets = json.load(f)

print(f"Seeding {len(tickets)} tickets into Redis...")

# Severity Mapping (Lower Score = Higher Priority)
priority_map = {
    "critical": 1,
    "high": 2,
    "medium": 3,
    "low": 4
}

for ticket in tickets:
    ticket_id = ticket['ticket_id']
    severity = ticket['severity']
    score = priority_map.get(severity, 5) # Default to 5 if unknown

    # 1. Store the FULL data in a Hash (Key: ticket:TICK-101)
    key_name = f"ticket:{ticket_id}"
    # We use hset to map the dictionary to the Redis Hash
    r.hset(key_name, mapping=ticket)
    
    # 2. Add ID to the Priority Queue (ZSET)
    r.zadd(QUEUE_NAME, {ticket_id: score})
    
    print(f"Stored {ticket_id} | Severity: {severity} | Queue Score: {score}")

print("\nDatabase seeding completed.")
