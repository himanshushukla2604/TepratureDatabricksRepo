# IoT to S3 Data Pipeline

This project generates IoT sensor data and batches it to AWS S3 when reaching the configured threshold.

## Architecture

```
IoT Data Generator → Batch Processor → S3 Storage
```

## Features

- **IoT Data Generation**: Creates realistic sensor data (temperature, humidity, battery, pressure, light)
- **Batch Processing**: Automatically batches records and uploads to S3 when threshold is reached
- **Configurable**: Environment-based configuration for all components
- **Mock Mode**: Can run without actual AWS S3 for testing

## Setup

### 1. Create Virtual Environment
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment
Copy `env_example.txt` to `.env` and fill in your configuration:
```bash
cp env_example.txt .env
```

Edit `.env` with your actual credentials:
- **AWS Credentials**: Access keys and region for S3
- **S3 Configuration**: Bucket name and prefix
- **Pipeline Settings**: Batch size and data generation interval

### 4. Setup Infrastructure
- **S3**: Create S3 bucket with appropriate permissions

## Usage

### Run the Pipeline
```bash
python main.py
```

The pipeline will:
1. Generate IoT sensor data every 5 seconds (configurable)
2. Collect records in memory until reaching batch size (default: 100)
3. Upload completed batches to S3 with timestamp and metadata
4. Continue the cycle

### Configuration Options

- `BATCH_SIZE`: Number of records before S3 upload (default: 100)
- `IOT_INTERVAL_SECONDS`: Time between data generation (default: 5)
- `S3_PREFIX`: S3 folder structure (default: iot-data/)

## Data Format

### Individual Records
```json
{
  "device_id": "sensor_001",
  "timestamp": "2025-01-15T10:30:00.123456",
  "temperature": 23.45,
  "humidity": 65.2,
  "battery": 87,
  "pressure": 1013.25,
  "light_level": 450
}
```

### Batch Files
```json
{
  "batch_timestamp": "20250115_103000",
  "record_count": 100,
  "records": [...]
}
```

## Project Structure
- `main.py`: Complete pipeline with all classes (IoTDataGenerator, S3BatchUploader, IoTDataPipeline)
- `iot_to_s3.py`: Legacy direct S3 uploader (kept for reference)
- `requirements.txt`: Python dependencies for AWS S3
- `env_example.txt`: Environment variables template
- `README.md`: This file
- `bronze_layer.ipynb`: consume from s3 store it in compressed fromat in raw layer s3
- `silver_layer.ipynb`: read data from raw and filter it and store it for futer analysis
- `gold_layer.ipynb`: perfored some analytics on silver layer data add some visualizations
- `device_health_index.ipynb`: this will contain all the visualization that we have performed regarding device health

## Classes in main.py

### IoTDataGenerator
- Generates realistic sensor data from 10 virtual devices
- Configurable sensor types and value ranges
- Timestamp-based data generation

### S3BatchUploader
- Manages S3 batch uploads with proper error handling
- Creates organized file structure with timestamps
- Mock mode for testing without AWS

### IoTDataPipeline
- Main orchestrator coordinating data generation and S3 batching
- Batch management and threshold monitoring
- Statistics tracking and graceful shutdown

# IoT Device MTTR & KPI Pipeline - README

This notebook processes IoT device telemetry data stored in Delta format on AWS S3, computes key metrics, and writes results to a gold table for analytics and visualization.

### Cell 1: MTTR Calculation
- Loads device telemetry from the silver Delta table.
- Casts columns to appropriate types.
- Detects status changes (healthy ↔ faulty) per device.
- Segments data into continuous runs of healthy/faulty states.
- Calculates Mean Time to Repair (MTTR) per device by measuring the duration of each faulty run until recovery.

### Cell 2: Battery Metrics
- Computes average battery level per device.
- Calculates the percentage of time each device's battery was below 30%.

### Cell 3: Fault Frequency
- Aggregates the number of faults per device per week.

### Cell 4: KPI Aggregation
- Joins MTTR, battery, and fault frequency metrics into a unified DataFrame.
- Aligns weekly fault counts with device and event date.

### Cell 5: Output & Visualization
- Displays the final KPI DataFrame.
- Writes results to the gold Delta table partitioned by event date.
- Provides explanations for recommended visualizations:
  1. MTTR per device
  2. Average battery level per device
  3. Low battery percentage per device
  4. Fault frequency per device per week

# IoT Device Uptime & Reliability Metrics Notebook -gold

This notebook calculates key reliability metrics for IoT devices using telemetry data stored in Delta format on AWS S3.

## Logic Overview

- **Load Data**: Reads device telemetry from the silver Delta table (`path`).
- **Event Boundaries**: Identifies the first and last event per device per day to estimate the expected number of readings.
- **Uptime Calculation**: Computes the percentage of actual readings versus expected readings for each device and day.
- **Fault Analysis**:
  - Counts the number of faults per device per day.
  - Calculates Mean Time Between Failures (MTBF) by measuring the average time between consecutive faults for each device.
- **Result Aggregation**: Joins uptime, fault count, and MTBF into a single DataFrame.
- **Output**: Displays the final metrics and writes them to the gold Delta table (`dest s3 path`), partitioned by event date.

## Output Columns

- `device_id`: Unique identifier for each device.
- `event_date`: Date of the telemetry events.
- `expected_readings`: Estimated number of expected readings per device per day.
- `actual_count`: Actual number of readings received.
- `uptime_percent`: Percentage of uptime based on readings.
- `mtbf_seconds`: Mean time between failures (in seconds).
- `faultCount`: Number of faults detected per device per day.

## Usage

Run this notebook to generate daily reliability KPIs for all devices. The results are stored in the gold table for further analytics and visualization.


