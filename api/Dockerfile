FROM python:3.10-slim

WORKDIR /app

# Install the async libraries needed for high-speed websockets
RUN pip install fastapi uvicorn websockets asyncpg

# Copy the API code into the container
COPY main.py /app/main.py

# Start the server on port 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]