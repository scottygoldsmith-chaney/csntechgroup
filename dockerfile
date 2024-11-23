# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable
ENV PORT 8080
ENV client_id = 'e6f85dc7b9563784d1458ababceaa18bbeca2108db55ab757bfb45b633bdb022'
ENV client_secret = 'pco_pat_9546fa8923b2ac1902aca67c3e9c64954a2eedbb6ad6b8884a268e94c96e61b3a684a485'
ENV dataset = 'csntechgroup.rct114'

# Run main-v2.py when the container launches
CMD ["python", "main-v2.py"]