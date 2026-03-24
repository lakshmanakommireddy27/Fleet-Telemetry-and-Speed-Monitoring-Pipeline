from kafka import KafkaProducer
from faker import Faker
import json
import random
from datetime import datetime
import time

fake = Faker()

producer = KafkaProducer(bootstrap_servers=['localhost:9093'], max_block_ms=5000)

locations = ["Highway 1", "Downtown", "Suburban Area", "Industrial Zone", "Rural Road"]

def generate_vehicle_data():
    vehicle_info = {
        "vehicle_id": fake.license_plate(),
        "make": fake.company(),
        "model": fake.word(),
        "color": fake.color_name(),
        "year": random.randint(2000, 2024),
    }

    data = {
        "timestamp": datetime.now().isoformat(),
        "location": random.choice(locations),
        "vehicle_info": vehicle_info,
        "speed": round(random.uniform(30.0, 150.0), 2),
        "vehicle_type": random.choice(["Car", "Truck", "Motorcycle", "Bus"]),
        "speed_limit": random.choice([50, 60, 80, 100, 120]),
        "traffic_density": random.choice(["Low", "Medium", "High"]),
        "road_condition": random.choice(["Dry", "Wet", "Icy"]),
        "weather_condition": random.choice(["Clear", "Rainy", "Foggy", "Snowy", "Windy"]),
        "incident_reported": random.choice([True, False]),
        "incident_details": fake.sentence() if random.choice([True, False]) else None
    }
    return data

while True:
    vehicle_data = generate_vehicle_data()
    producer.send('vehicle-data', json.dumps(vehicle_data).encode('utf-8'))
    print(f"Sent: {vehicle_data}")
    time.sleep(1)  # Giả lập gửi dữ liệu mỗi 1 giây
