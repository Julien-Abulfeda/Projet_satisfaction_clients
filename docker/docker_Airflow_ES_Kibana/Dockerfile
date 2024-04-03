# Use the official Airflow image as the base
FROM apache/airflow:2.8.4

# Copy the requirements.txt file into the Docker image
COPY requirements.txt requirements.txt
# RUN pip freeze >> res.txt
# Install the requirements
RUN pip install -r requirements.txt
