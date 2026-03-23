FROM python:3.10-slim

WORKDIR /app

ARG SERVICE_NAME

# Copy and install common requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared module
COPY shared/ ./shared/

# Copy service code
COPY services/${SERVICE_NAME}/ ./service/

WORKDIR /app/service

ENV PYTHONPATH=/app
