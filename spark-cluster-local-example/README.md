# Spark Cluster Local Example

[Ref] https://blog.devgenius.io/running-a-pyspark-etl-pipeline-on-a-local-spark-cluster-with-docker-25a4f286305b

A complete local Apache Spark cluster setup using Docker Compose, designed for development, testing, and learning purposes. This project provides a multi-node Spark cluster with a master node, scalable workers, and a history server, along with an example ETL pipeline processing NYC TLC taxi data.

## Features

- 🐳 **Docker-based**: Fully containerized Spark cluster with Docker Compose
- 🔧 **Easy Setup**: Single command to build and start the cluster
- 📈 **Scalable**: Dynamically scale workers based on your needs
- 📊 **Monitoring**: Built-in Spark Master UI and History Server
- 💾 **Persistent Storage**: Volume mounts for data and logs
- 🚀 **Example ETL**: Complete NYC taxi data pipeline included
- 🐍 **PySpark Ready**: Pre-configured Python environment with PySpark
- 📝 **Makefile**: Convenient commands for common operations

## Project Structure

```
.
├── conf/
│   └── spark-defaults.conf     # Spark configuration
├── data/
│   ├── yellow_tripdata_2024-01.parquet  # Raw trip data
│   ├── taxi_zone_lookup.csv             # Zone lookup data
│   └── results/                         # Output directory
├── spark_apps/
│   └── taxi_etl.py            # Example ETL application
├── Dockerfile                 # Spark container image
├── docker-compose.yml         # Cluster orchestration
├── entrypoint.sh              # Container startup script
├── main.py                    # Local results viewer
├── Makefile                   # Build and run commands
├── pyproject.toml             # Python project config
└── requirements.txt           # Python dependencies
```

## Quick Start

### 1. Download Sample Data

```bash
mkdir -p data
cd data

# Download NYC TLC Yellow Taxi data (January 2024)
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

# Download taxi zone lookup table
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

cd ..
```

### 2. Build the Docker Image

```bash
make build
# or
docker compose build
```

### 3. Start the Cluster

```bash
# Start with 1 worker (default)
make run

# Start with 3 workers
make run-scaled

# Or run in detached mode
make run-d
```

### 4. Submit the Example Job

```bash
# Using Makefile
make submit app=taxi_etl.py

# Or directly with Docker
docker exec -it da-spark-master \
  spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/apps/taxi_etl.py
```

### 5. View Results

After the job completes, view results locally:

```bash
python main.py
```

This will display:
- Schema of curated trip data
- Schema of daily KPIs
- Top 10 trips by total amount
- Top 20 zones by trip count

## Cluster Components

### Spark Master (`da-spark-master`)
- **Role**: Cluster manager and job scheduler
- **UI**: http://localhost:9090
- **Ports**: 9090 (UI), 7077 (Spark)

### Spark Workers
- **Role**: Execute tasks and store RDD partitions
- **Scalable**: Start with `--scale spark-worker=N`

### Spark History Server (`da-spark-history`)
- **Role**: View completed application logs
- **UI**: http://localhost:18080
- **Reads from**: `/opt/spark/spark-events`

## Available Commands

| Command | Description |
|---------|-------------|
| `make build` | Build Docker images |
| `make build-nc` | Build without cache |
| `make run` | Start cluster (stops existing first) |
| `make run-scaled` | Start with 3 workers |
| `make run-d` | Start in detached mode |
| `make stop` | Stop containers |
| `make down` | Stop and remove containers/volumes |
| `make submit app=<name>` | Submit Spark application |
| `make rm-results` | Clear output results |

## Example Application: NYC Taxi ETL

The included `taxi_etl.py` demonstrates a complete ETL pipeline:

**Input:**
- Yellow taxi trip data (Parquet)
- Taxi zone lookup table (CSV)

**Transformations:**
- Parse timestamps and dates
- Calculate trip duration and average speed
- Filter invalid/outlier records
- Enrich with zone information (borough, zone name)

**Outputs:**
1. **Curated Trip Data** (`data/results/curated_taxi_trips/`)
   - Partitioned by pickup date
   - Contains enriched trip records

2. **Daily KPIs** (`data/results/daily_zone_kpis/`)
   - Aggregated metrics per zone per day
   - Includes: trip count, revenue, avg distance, avg duration

## Configuration

### Spark Defaults (`conf/spark-defaults.conf`)

```properties
spark.master                     spark://spark-master:7077
spark.eventLog.enabled          true
spark.eventLog.dir              /opt/spark/spark-events
spark.history.fs.logDirectory   /opt/spark/spark-events
```

### Environment Variables

Configure in `.env.spark` file (create if needed):

```bash
SPARK_WORKER_MEMORY=2g
SPARK_WORKER_CORES=2
SPARK_DRIVER_MEMORY=1g
```

## Development Workflow

### Iterating on Spark Applications

1. Place your PySpark scripts in `spark_apps/`
2. Submit jobs using `make submit app=your_app.py`
3. Monitor progress in Master UI (http://localhost:9090)
4. Review completed jobs in History Server (http://localhost:18080)

### Viewing Logs

```bash
# Follow master logs
docker logs -f da-spark-master

# Follow worker logs
docker logs -f da-spark-worker-1

# View specific container
docker compose logs spark-worker
```

### Debugging

```bash
# Access master container shell
docker exec -it da-spark-master bash

# Check Spark processes
docker exec da-spark-master jps

# View Spark configuration
docker exec da-spark-master cat /opt/spark/conf/spark-defaults.conf
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│                 Docker Network                   │
├─────────────────────────────────────────────────┤
│                                                  │
│  ┌──────────────┐      ┌──────────────┐        │
│  │ Spark Master │──────│   Worker 1   │        │
│  │   (port 7077) │      │              │        │
│  │   (port 9090) │      └──────────────┘        │
│  └──────────────┘              │                │
│         │                      │                │
│         │              ┌──────────────┐        │
│         │              │   Worker 2   │        │
│         │              │              │        │
│         │              └──────────────┘        │
│         │                      │                │
│         ▼                      ▼                │
│  ┌──────────────────────────────────┐          │
│  │      Shared Volume Mounts        │          │
│  │  • ./data → /opt/spark/data      │          │
│  │  • ./spark_apps → /opt/spark/apps│          │
│  │  • spark-logs (event logs)       │          │
│  └──────────────────────────────────┘          │
│         ▲                                       │
│         │                                       │
│  ┌──────────────┐                              │
│  │   History    │                              │
│  │   Server     │                              │
│  │ (port 18080) │                              │
│  └──────────────┘                              │
└─────────────────────────────────────────────────┘
```

## Technology Stack

- **Apache Spark**: 3.3.3
- **Python**: 3.10 (container), 3.12 (local)
- **Java**: OpenJDK 11
- **Base OS**: Debian Bullseye
- **PySpark**: 4.1.1
- **Supporting Libraries**: pandas, pyarrow, ipython

## Troubleshooting

### Container won't start
```bash
# Clean up and rebuild
make down
make build-nc
make run
```

### Out of memory errors
- Increase Docker memory limit (Docker Desktop → Settings → Resources)
- Reduce worker count or memory allocation

### Job fails to submit
- Ensure master is healthy: `docker compose ps`
- Check master logs: `docker logs da-spark-master`
- Verify app path: files should be in `spark_apps/` directory

### Can't access UIs
- Wait for containers to fully start (health checks)
- Check port conflicts: `netstat -an | grep 9090`

## Data Sources

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Yellow Taxi: January 2024
- Zone Lookup: TLC official zone mapping

## License

This project is provided for educational and development purposes. Data used is from NYC TLC public datasets.

## Contributing

Feel free to submit issues and enhancement requests!
