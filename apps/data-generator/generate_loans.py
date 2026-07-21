import argparse
import pandas as pd
from faker import Faker
import random
import os

fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_loans(num_rows, output_dir):
    print(f"Generating {num_rows} loan applications...")
    
    data = []
    for _ in range(num_rows):
        loan_id = fake.uuid4()
        customer_id = fake.uuid4()
        ssn = fake.ssn()
        name = fake.name()
        email = fake.email()
        address = fake.address().replace('\n', ', ')
        
        # Financials
        credit_score = random.randint(300, 850)
        loan_amount = round(random.uniform(50000, 1000000), 2)
        property_value = round(loan_amount * random.uniform(1.0, 1.3), 2) # Property usually worth more than loan
        interest_rate = round(random.uniform(2.5, 8.5), 3)
        
        # Categorical
        loan_purpose = random.choice(['Purchase', 'Refinance', 'Debt Consolidation'])
        status = random.choice(['Approved', 'Denied', 'Pending', 'Closed'])
        application_date = fake.date_between(start_date='-2y', end_date='today').isoformat()
        
        data.append({
            'loan_id': loan_id,
            'customer_id': customer_id,
            'ssn': ssn,
            'name': name,
            'email': email,
            'address': address,
            'credit_score': credit_score,
            'loan_amount': loan_amount,
            'property_value': property_value,
            'interest_rate': interest_rate,
            'loan_purpose': loan_purpose,
            'status': status,
            'application_date': application_date
        })
        
    df = pd.DataFrame(data)
    
    # Ensure directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, 'loan_applications.csv')
    df.to_csv(output_path, index=False)
    print(f"Successfully generated data at: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate mock loan applications data.")
    parser.add_argument('--rows', type=int, default=1000, help="Number of rows to generate.")
    parser.add_argument('--outdir', type=str, default='../mortgage-data-platform/data/raw/loans', help="Output directory.")
    args = parser.parse_args()
    
    generate_loans(args.rows, args.outdir)
