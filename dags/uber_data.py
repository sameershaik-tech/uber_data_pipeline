from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from faker import Faker
import random

# Initialize Faker
fake = Faker()

def generate_random_uber_data():
    # Generate random data using Faker
    ride_id = fake.uuid4()
    pickup_location = f"{fake.latitude()}, {fake.longitude()}"
    dropoff_location = f"{fake.latitude()}, {fake.longitude()}"
    pickup_time = fake.date_time_this_year()
    dropoff_time = pickup_time + timedelta(minutes=random.randint(5, 30))  # Simulate ride duration
    date_collected = pickup_time.date()
    driver_name = fake.name()
    passenger_name = fake.name()
    fare_amount = round(random.uniform(10.0, 100.0), 2)  # Simulate fare amount between $10 and $100
    ride_duration = round((dropoff_time - pickup_time).total_seconds() / 60, 2)  # Duration in minutes

    # Return the generated data
    return {
        'ride_id': ride_id,
        'pickup_location': pickup_location,
        'dropoff_location': dropoff_location,
        'pickup_time': pickup_time,
        'dropoff_time': dropoff_time,
        'date_collected': date_collected,
        'driver_name': driver_name,
        'passenger_name': passenger_name,
        'fare_amount': fare_amount,
        'ride_duration': ride_duration,
    }

def create_table_if_not_exists():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    cur = conn.cursor()  
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS uber_data (
        id SERIAL PRIMARY KEY,
        ride_id VARCHAR(255),
        pickup_location VARCHAR(255),
        dropoff_location VARCHAR(255),
        pickup_time TIMESTAMP,
        dropoff_time TIMESTAMP,
        date_collected DATE,
        driver_name VARCHAR(255),
        passenger_name VARCHAR(255),
        fare_amount DECIMAL(10, 2),
        ride_duration DECIMAL(10, 2)
    )
    """
    
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()

def insert_data_into_postgres(**kwargs):
    # Retrieve the data from the task instance
    data = kwargs['ti'].xcom_pull(task_ids='generate_data')
    
    # Connect to PostgreSQL
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Insert data into PostgreSQL
    insert_query = """ 
    INSERT INTO uber_data (
        ride_id, pickup_location, dropoff_location, pickup_time, dropoff_time, date_collected,
        driver_name, passenger_name, fare_amount, ride_duration
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cur.execute(insert_query, (
        data['ride_id'],
        data['pickup_location'],
        data['dropoff_location'],
        data['pickup_time'],
        data['dropoff_time'],
        data['date_collected'],
        data['driver_name'],
        data['passenger_name'],
        data['fare_amount'],
        data['ride_duration']
    ))

    conn.commit()
    cur.close()
    conn.close()

# Define the default arguments
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1, 0, 0),  # Specific start date and time
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'fetch_and_insert_uber_data',
    default_args=default_args,
    description='Fetch random Uber data, create table if not exists, and insert into PostgreSQL',
    schedule_interval='* * * * *',  # Run every minute
    catchup=False,  # Avoid backfilling if not needed
)

# Define the tasks
create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table_if_not_exists,
    dag=dag,
)

generate_data = PythonOperator(
    task_id='generate_data',
    python_callable=generate_random_uber_data,
    dag=dag,
)

insert_data = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data_into_postgres,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
create_table >> generate_data >> insert_data
