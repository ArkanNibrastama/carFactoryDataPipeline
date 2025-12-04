# Use an official Python runtime as a parent image.
# The 'slim' version is smaller and good for production.
FROM python:3.9-slim

# Set the working directory in the container to /app
WORKDIR /app

# Copy the requirements file into the container at /app
# This is done as a separate step to leverage Docker's layer caching.
# This layer will only be rebuilt if requirements.txt changes.
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
# --no-cache-dir makes the image smaller.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container at /app
COPY process.py .

# Define the command to run your app when the container launches.
# This will execute 'python process.py' and start the SQS polling loop.
CMD ["python", "process.py"]
