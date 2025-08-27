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

## Monitoring

The pipeline provides detailed logging:
- Data generation status
- Batch processing and S3 upload confirmations
- Error handling and connection status
- Performance metrics (total records, batches)

## Testing Without Infrastructure

The pipeline includes mock implementations for testing:
- **Mock S3 Client**: Logs uploads instead of writing to S3

This allows you to test the data generation and batching logic without setting up AWS.

## Troubleshooting

### Common Issues
1. **Import Errors**: Ensure you're in the correct directory and virtual environment
2. **S3 Permissions**: Verify AWS credentials and bucket permissions
3. **Environment Variables**: Ensure `.env` file is properly configured

### Debug Mode
Set logging level to DEBUG for more detailed information:
```python
logging.basicConfig(level=logging.DEBUG)
```
