FROM apache/airflow:3.0.6-python3.11

# Copy requirements file and install with airflow user (default)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
