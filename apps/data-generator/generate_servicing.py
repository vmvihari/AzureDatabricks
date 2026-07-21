import argparse
import pandas as pd
from faker import Faker
import random
import os
import datetime

fake = Faker()
Faker.seed(123)
random.seed(123)

def generate_servicing_events(num_rows, output_dir):
    print(f"Generating {num_rows} CDC servicing events...")
    
    data = []
    for _ in range(num_rows):
        customer_id = fake.uuid4()
        
        # Simulate an update event (address change or credit score change)
        event_type = random.choice(['ADDRESS_UPDATE', 'CREDIT_SCORE_UPDATE'])
        
        if event_type == 'ADDRESS_UPDATE':
            new_value = fake.address().replace('\n', ', ')
            column_affected = 'address'
        else:
            new_value = str(random.randint(300, 850))
            column_affected = 'credit_score'
            
        event_timestamp = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30))
        
        data.append({
            'event_id': fake.uuid4(),
            'customer_id': customer_id,
            'event_type': event_type,
            'column_affected': column_affected,
            'new_value': new_value,
            'event_timestamp': event_timestamp.isoformat()
        })
        
    df = pd.DataFrame(data)
    
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'servicing_events.csv')
    df.to_csv(output_path, index=False)
    print(f"Successfully generated CDC data at: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate mock Change Data Capture (CDC) events for servicing.")
    parser.add_argument('--rows', type=int, default=200, help="Number of events to generate.")
    parser.add_argument('--outdir', type=str, default='../mortgage-data-platform/data/raw/servicing', help="Output directory.")
    args = parser.parse_args()
    
    generate_servicing_events(args.rows, args.outdir)
