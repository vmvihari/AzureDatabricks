import argparse
import json
from faker import Faker
import random
import os

fake = Faker()
Faker.seed(99)
random.seed(99)

def generate_fraud_blacklist(num_rows, output_dir):
    print(f"Generating {num_rows} fraudulent entities...")
    
    fraud_data = []
    for _ in range(num_rows):
        entity = {
            "fraud_id": fake.uuid4(),
            "entity_type": random.choice(["SSN", "Email", "IP_Address"]),
            "risk_score": random.randint(70, 100),
            "reported_date": fake.date_between(start_date='-5y', end_date='today').isoformat(),
            "details": {
                "source": random.choice(["Internal", "FBI Database", "External Vendor"]),
                "notes": fake.sentence()
            }
        }
        
        # Populate the actual malicious value based on type
        if entity["entity_type"] == "SSN":
            entity["malicious_value"] = fake.ssn()
        elif entity["entity_type"] == "Email":
            entity["malicious_value"] = fake.email()
        else:
            entity["malicious_value"] = fake.ipv4()
            
        fraud_data.append(entity)
        
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'fraud_blacklist.json')
    
    with open(output_path, 'w') as f:
        json.dump(fraud_data, f, indent=4)
        
    print(f"Successfully generated fraud data at: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate mock fraud blacklist data in JSON format.")
    parser.add_argument('--rows', type=int, default=500, help="Number of fraudulent entities to generate.")
    parser.add_argument('--outdir', type=str, default='../mortgage-data-platform/data/raw/fraud', help="Output directory.")
    args = parser.parse_args()
    
    generate_fraud_blacklist(args.rows, args.outdir)
