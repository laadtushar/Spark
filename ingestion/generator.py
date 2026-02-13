import json
import time
import random
import logging
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("generator")

fake = Faker()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "job.postings.v1"

# Tech stack for weighted generation
TECH_SKILLS = ["python", "spark", "kafka", "react", "aws", "kubernetes", "docker", "sql", "java", "golang"]
LEVELS = ["Junior", "Mid", "Senior", "Lead", "Staff"]
LOCATIONS = ["London, UK", "Newcastle, UK", "New York, US", "San Francisco, US", "Remote", "Berlin, DE", "Singapore, SG"]

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_job():
    title_skill = random.choice(TECH_SKILLS).capitalize()
    level = random.choice(LEVELS)
    
    # Ensuring relevant skills appear in the description for Spark extraction
    skills = random.sample(TECH_SKILLS, random.randint(2, 5))
    description = f"We are looking for a {level} {title_skill} Engineer. " \
                  f"Required skills: {', '.join(skills)}. " \
                  f"Experience with {random.choice(TECH_SKILLS)} is a plus."
    
    return {
        "event_id": fake.uuid4(),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "simulated_scrapper",
        "payload": {
            "job_id": str(random.randint(100000, 999999)),
            "title": f"{level} {title_skill} Engineer",
            "company": fake.company(),
            "location": random.choice(LOCATIONS),
            "description": description,
            "is_remote": random.choice([True, False])
        }
    }

def main():
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'job-gen-01'
    }
    
    producer = Producer(producer_conf)
    
    logger.info(f"Starting generator. Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    
    try:
        while True:
            job_data = generate_job()
            producer.produce(
                TOPIC, 
                key=job_data["event_id"], 
                value=json.dumps(job_data).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0) # Serve delivery callbacks
            
            logger.info(f"Produced: {job_data['payload']['title']} at {job_data['payload']['company']}")
            
            # Frequent updates to mimic high-frequency
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        logger.info("Stopping generator...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
