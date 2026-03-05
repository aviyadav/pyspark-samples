# Base image: Python 3.10 on Debian Bullseye
# We need Python for PySpark and a stable Linux OS
FROM python:3.10-bullseye as spark-base

# Spark version is configurable at build time
ARG SPARK_VERSION=3.3.3

# Install OS-level tools and dependencies
# - Java 11: required by Spark
# - curl: download Spark binaries
# - build-essential: compile Python native deps
# - ssh: expected by Spark standalone scripts
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    sudo \
    curl \
    vim \
    unzip \
    rsync \
    openjdk-11-jdk \
    build-essential \
    software-properties-common \
    ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Define standard installation paths for Spark and Hadoop
# Hadoop libs are needed even without running HDFS
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}


# Create required directories and set Spark as working directory
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download and install Apache Spark
# --strip-components=1 avoids nested spark-<version> folders
RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
    && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz



# Install Python dependencies required by Spark jobs
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Add Spark binaries to PATH so spark-submit, start-master.sh, etc. work
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

# Default Spark standalone master configuration
# "spark-master" is resolved via Docker internal DNS
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077

# Force Spark to use Python 3
ENV PYSPARK_PYTHON python3

# Copy Spark default configuration (event logs, tuning, etc.)
COPY conf/spark-defaults.conf "$SPARK_HOME/conf"

# Ensure Spark shell scripts are executable
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*


# Make Spark's Python libraries discoverable
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Copy container entrypoint that decides the role:
# master / worker / history server
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh


# Run entrypoint on container start
ENTRYPOINT ["./entrypoint.sh"]