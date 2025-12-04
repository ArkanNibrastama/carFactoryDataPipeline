# Use the official AWS-provided base image for Python 3.9 Lambda functions.
# This image includes the Lambda Runtime Interface Client, which is required.
FROM public.ecr.aws/lambda/python:3.9

# Copy the requirements file into the container
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code (main.py) into the container
COPY main.py .

# The CMD specifies the handler for the Lambda function.
# The format is <filename>.<handler_variable_name>.
# In our main.py, the Mangum handler variable is named 'handler'.
CMD [ "main.handler" ]
