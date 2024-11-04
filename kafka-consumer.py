from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime
import logging
import time
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class UserLoginProcessor:
    def __init__(self, bootstrap_servers='localhost:29092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = self._create_consumer()
        self.producer = self._create_producer()
        
        # State for analytics
        self.device_stats = defaultdict(int)
        self.version_stats = defaultdict(int)
        self.location_stats = defaultdict(int)
        self.last_processed_time = time.time()
        self.processed_count = 0

    def _create_consumer(self):
        """Create and return a Kafka consumer"""
        try:
            return KafkaConsumer(
                'user-login',
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                group_id='user_login_processor',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                enable_auto_commit=True,
                auto_commit_interval_ms=5000
            )
        except Exception as e:
            logging.error(f"Failed to create consumer: {str(e)}")
            raise

    def _create_producer(self):
        """Create and return a Kafka producer"""
        try:
            return KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
        except Exception as e:
            logging.error(f"Failed to create producer: {str(e)}")
            raise

    def process_message(self, message):
        """Process a single message and return enriched data
        Example message:
            Topic: user-login, Partition: 0, Offset: 38296, Key: b'ad6ecb39-f341-4cdf-bc7f-110af22b940b', 
            Value: {"user_id": "89904f32-8677-4aff-a398-91dfca4cad02", "app_version": "2.3.0", "ip": "152.183.203.100", "locale": "IL", 
                    "device_id": "6d12184b-b7b1-44f9-9dd7-8114a0e1f3e7", "timestamp": 1730575000, "device_type": "android"}

        """
        try:
            # Convert Unix timestamp to readable format
            timestamp = datetime.fromtimestamp(int(message['timestamp']))
            
            # Enrich the data
            enriched_data = {
                **message,
                'processed_timestamp': datetime.now().isoformat(),
                'readable_timestamp': timestamp.isoformat(),
                'hour_of_day': timestamp.hour
            }
            
            # Update statistics
            self.device_stats[message.get('device_type', 'unknown')] += 1
            self.version_stats[message.get('app_version', 'unknown')] += 1
            self.location_stats[message.get('locale', 'unknown')] += 1
            
            return enriched_data
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
            return None

    def aggregate_statistics(self):
        return {"current_time": self.last_processed_time,
                "time_window": self.elapsed_time , 
                "batch_size": self.messages_per_second , 
                "location_stats": dict(self.location_stats), 
                "device_stats":dict(self.device_stats)}
    
    def print_statistics(self):
        """Print current statistics"""

        logging.info("\n=== Processing Statistics ===")
        logging.info(f"Messages processed in last {self.elapsed_time:.2f} seconds: {self.processed_count}")
        logging.info(f"Processing rate: {self.messages_per_second:.2f} messages/second")
        logging.info("\nTop Devices:")
        for device, count in sorted(self.device_stats.items(), key=lambda x: x[1], reverse=True):
            logging.info(f"- {device}: {count}")
        logging.info("\nTop Locations:")
        for locale, count in sorted(self.location_stats.items(), key=lambda x: x[1], reverse=True)[:3]:
            logging.info(f"- {locale}: {count}")
        
        

    def run(self):
        """Main processing loop"""
        logging.info("Starting the user login processor...")
        
        try:
            while True:
                message_batch = self.consumer.poll(timeout_ms=1000)
                for partition_batch in message_batch.values():
                    for message in partition_batch:
                        # Process message
                        enriched_data = self.process_message(message.value)
                        if enriched_data:
                            # Send to processed topic
                            self.producer.send('processed-logins', enriched_data)
                            self.processed_count += 1
                
                # Print statistics every 10 seconds
                if time.time() - self.last_processed_time >= 10:
                    
                    current_time = time.time()
                    self.elapsed_time = current_time - self.last_processed_time
                    self.messages_per_second = self.processed_count / self.elapsed_time if self.elapsed_time > 0 else 0
                    
                    # self.print_statistics()
                    # Log to producer
                    print(self.aggregate_statistics())
                    self.producer.send('aggregated-usage', self.aggregate_statistics())
                    # Reset counters
                    self.last_processed_time = current_time
                    self.processed_count = 0
                    self.device_stats = defaultdict(int)
                    self.version_stats = defaultdict(int)
                    self.location_stats = defaultdict(int)

                    
                    
                # Flush the producer
                self.producer.flush()
                
        except KeyboardInterrupt:
            logging.info("Shutting down processor...")
        except Exception as e:
            logging.error(f"Fatal error: {str(e)}")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    processor = UserLoginProcessor()
    processor.run()
