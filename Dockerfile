FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code (only what we need)
COPY mrci_data.py .

# Make sure the persistent profile directory exists
RUN mkdir -p mrci_profile

# Ensure output is not buffered
ENV PYTHONUNBUFFERED=1

# Run the update script directly
CMD ["python", "mrci_data.py"]
