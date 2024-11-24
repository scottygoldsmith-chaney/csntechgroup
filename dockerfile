# Use Python slim base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy all files to container
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the required port for Cloud Run
EXPOSE 8080

# Set the entry point for the container
CMD ["python", "main.py"]
