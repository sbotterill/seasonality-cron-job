FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the job scripts
COPY databento_data.py ./
COPY test_databento.py ./

# Default command - fetch last 7 days
CMD ["python", "databento_data.py"]
