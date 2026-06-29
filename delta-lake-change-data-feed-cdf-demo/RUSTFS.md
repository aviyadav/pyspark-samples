# Running the Delta CDF demo on RustFS in Docker

This document explains what needs to change to run this PySpark + Delta Lake
CDF demo with table data stored in [RustFS](https://github.com/rustfs/rustfs)
running in Docker.

## Accuracy review of the original notes

The previous guidance was directionally correct, but not complete enough to run
unchanged:

- ✅ Correct: RustFS is S3-compatible, so Spark should access it through
  Hadoop's `s3a://` filesystem.
- ✅ Correct: Delta table paths and streaming checkpoint paths must move from
  local `delta/...` paths to `s3a://bucket/...` paths.
- ✅ Correct: CDF must be enabled on the initial Delta table write.
- ⚠️ Incomplete: the Docker command used placeholder image/flags. The official
  image is `rustfs/rustfs`, and the quickstart exposes API port `9000` and
  console port `9001`.
- ⚠️ Incomplete: Spark does **not** always include the S3A connector by default.
  This project uses PySpark `4.1.1`, whose bundled Hadoop version is `3.4.2`, so
  the demo needs the matching `org.apache.hadoop:hadoop-aws:3.4.2` package plus
  AWS SDK dependencies resolved by Maven.
- ⚠️ Incomplete: the target S3 bucket must exist before Spark writes to
  `s3a://bucket/...`.
- ⚠️ Needs validation: RustFS S3 compatibility should be tested with Delta
  overwrite, merge/update/delete, CDF batch reads, and streaming checkpoints.
  Delta relies on correct object-store list/read/write behavior and safe commit
  handling.

The sections below include the additional details needed to make it work.

---

## Recommended architecture

Use RustFS as an **S3-compatible object store** and access it from Spark through
Hadoop S3A:

```text
main.py / PySpark / Delta Lake
        |
        | s3a://delta-cdf-demo/customers
        v
Hadoop S3A connector
        |
        | http://localhost:9000
        v
RustFS Docker container
```

Avoid trying to use a custom `rustfs://` URI unless RustFS provides a Hadoop
`FileSystem` implementation for that scheme and you add its JARs to Spark.

---

## 1. Start RustFS with Docker

RustFS runs as a non-root user in the container (`10001:10001`). Host-mounted
folders must be writable by that UID/GID.

```bash
mkdir -p data logs
sudo chown -R 10001:10001 data logs

docker run -d \
  --name rustfs \
  -p 9000:9000 \
  -p 9001:9001 \
  -v $(pwd)/data:/data \
  -v $(pwd)/logs:/logs \
  rustfs/rustfs:latest
```

Ports:

| Port | Purpose |
|---|---|
| `9000` | S3-compatible API endpoint |
| `9001` | RustFS web console |

Open the console:

```text
http://localhost:9001
```

Default credentials from the RustFS quickstart:

```text
username: rustfsadmin
password: rustfsadmin
```

> If your deployment uses different credentials, use those instead in the Spark
> config below.

---

## 2. Create a bucket

Create a bucket before running Spark. Example bucket name used below:

```text
delta-cdf-demo
```

You can create it from the RustFS console or with an S3-compatible client.
Using AWS CLI:

```bash
aws configure set aws_access_key_id rustfsadmin
aws configure set aws_secret_access_key rustfsadmin
aws configure set default.region us-east-1

aws --endpoint-url http://localhost:9000 s3 mb s3://delta-cdf-demo
aws --endpoint-url http://localhost:9000 s3 ls
```

If `aws s3 mb` fails, confirm:

- the container is running: `docker ps`
- port `9000` is reachable
- the access key / secret key match your RustFS configuration
- the bucket name is valid and not already created

---

## 3. Add the S3A connector to Spark

This project uses:

| Component | Version |
|---|---|
| PySpark | `4.1.1` |
| Hadoop bundled with PySpark | `3.4.2` |
| Delta Spark | `4.3.0` |

For `s3a://` paths, Spark needs Hadoop's S3A connector. Add the matching
`hadoop-aws` package when configuring Delta:

```python
spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["org.apache.hadoop:hadoop-aws:3.4.2"],
).getOrCreate()
```

Why this matters:

- Without this package, Spark may fail with `ClassNotFoundException:
  org.apache.hadoop.fs.s3a.S3AFileSystem`.
- The `hadoop-aws` version should match Hadoop's runtime version. For this
  environment that is `3.4.2`.
- Maven must be reachable the first time Spark starts so it can download the
  Delta and Hadoop AWS JARs.

If your PySpark/Hadoop version changes, check the version with:

```bash
uv run python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[1]').getOrCreate(); print(s.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()); s.stop()"
```

Then use the matching `org.apache.hadoop:hadoop-aws:<that-version>` package.

---

## 4. Configure Spark for RustFS S3A

Update the Spark builder in `main.py` before calling
`configure_spark_with_delta_pip(...)`.

```python
builder = (
    SparkSession.builder.appName("DeltaCDF-Demo")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    # RustFS S3-compatible endpoint
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    # Required on Hadoop 3.4.x (AWS SDK v2): give S3A a region so it does not
    # try to auto-resolve one and fail against a non-AWS endpoint. Any valid
    # region string works for an S3-compatible store; "us-east-1" is typical.
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.access.key", "rustfsadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "rustfsadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["org.apache.hadoop:hadoop-aws:3.4.2"],
).getOrCreate()
```

Important notes:

- `fs.s3a.impl` explicitly registers the S3A filesystem class. It is often
  inferred automatically once `hadoop-aws` is on the classpath, but setting it
  makes configuration failures easier to diagnose.
- `path.style.access=true` is usually required for local S3-compatible object
  stores because `s3a://bucket/path` must resolve through
  `http://localhost:9000/bucket/path`, not `http://bucket.localhost:9000/path`.
- `fs.s3a.endpoint.region` is required on this stack. PySpark `4.1.1` bundles
  Hadoop `3.4.2`, whose `hadoop-aws` uses **AWS SDK v2**. SDK v2 refuses to
  start without a resolvable region and will not infer one from a custom
  endpoint, so omitting this typically causes
  `Unable to load region from any of the providers`. (Older Hadoop 3.3.x with
  AWS SDK v1 did not need this, which is why many older guides skip it.)
- For HTTPS RustFS, change the endpoint to `https://...` and set
  `spark.hadoop.fs.s3a.connection.ssl.enabled=true`.
- `http://localhost:9000` works when `main.py` runs on the host machine. If you
  run Spark/Python inside another Docker container, `localhost` points to that
  container, not to RustFS. Put both containers on the same Docker network and
  use an endpoint such as `http://rustfs:9000`, or use your host's reachable
  address.
- Do not hardcode production credentials. For a real deployment, read them from
  environment variables or a secrets manager.

---

## 5. Replace local paths with S3A paths

The current local demo uses paths like:

```python
"delta/customers"
"delta/customer_features"
"delta/customer_features_stream"
"delta/checkpoints/customer_sync"
```

For RustFS, define path constants and use them everywhere:

```python
BUCKET = "delta-cdf-demo"
SOURCE_PATH = f"s3a://{BUCKET}/customers"
FEATURES_PATH = f"s3a://{BUCKET}/customer_features"
STREAM_PATH = f"s3a://{BUCKET}/customer_features_stream"
CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/customer_sync"
```

Then update every `load`, `save`, `forPath`, `isDeltaTable`, and streaming
`checkpointLocation` call to use the constants:

```python
.save(SOURCE_PATH)
DeltaTable.forPath(spark, SOURCE_PATH)
.load(SOURCE_PATH)
run_incremental_sync(SOURCE_PATH, FEATURES_PATH, last_processed_version)
.option("checkpointLocation", CHECKPOINT_PATH)
.start(STREAM_PATH)
spark.read.format("delta").load(STREAM_PATH)
```

### What about `os.makedirs("delta")`?

For S3A/RustFS mode, local directory creation is not needed. The current demo's
`os.makedirs("delta", exist_ok=True)` is harmless only for local mode, but it
does not create S3 prefixes or buckets. For RustFS mode, create the bucket first
and use S3A paths.

---

## 6. Clean re-runs on RustFS

For the local demo, deleting `delta/` resets everything. For RustFS, delete the
bucket contents instead:

```bash
aws --endpoint-url http://localhost:9000 s3 rm s3://delta-cdf-demo --recursive
```

Then recreate the bucket if needed:

```bash
aws --endpoint-url http://localhost:9000 s3 mb s3://delta-cdf-demo
```

Why this matters:

- The streaming checkpoint under `s3a://delta-cdf-demo/checkpoints/...` remembers
  processed versions.
- Reusing old checkpoints can make the stream skip already-processed CDF events.
- Existing Delta tables may affect overwrite/merge behavior on re-runs.

---

## 7. Expected validation steps

After running:

```bash
uv run python main.py
```

Verify table files exist in RustFS:

```bash
aws --endpoint-url http://localhost:9000 s3 ls s3://delta-cdf-demo/ --recursive
```

You should see objects similar to:

```text
customers/_delta_log/00000000000000000000.json
customers/_delta_log/00000000000000000001.json
customers/part-....snappy.parquet
customer_features/_delta_log/...
customer_features_stream/_delta_log/...
checkpoints/customer_sync/...
```

Validate the demo output:

- Step 4 prints CDF rows with `_change_type`, `_commit_version`, and
  `_commit_timestamp`.
- Step 5 prints the incremental sync checkpoint.
- Step 6 prints rows from `s3a://delta-cdf-demo/customer_features_stream`.

---

## 8. Delta Lake and RustFS compatibility considerations

RustFS is S3-compatible, but Delta Lake workloads require more than simple
upload/download operations. Test these behaviors before treating this as
production-ready:

- `PUT`, `GET`, `LIST`, `HEAD`, and `DELETE` object operations.
- Consistent listing for `_delta_log/` files.
- Safe creation of new Delta commit JSON files.
- Correct behavior for overwrite, append, update, delete, merge, batch CDF read,
  and streaming checkpoint writes.

When Spark writes Delta to `s3a://`, Delta's S3 integration and Hadoop S3A handle
most of the object-store interaction. However, object-store commit semantics are
still important. Avoid multiple concurrent writers until you have verified
conflict handling on your RustFS deployment.

If you write to the same tables from Rust code, use the official `deltalake`
Rust crate or another Delta-aware writer. Do **not** write raw Parquet files into
the table directory without committing them through `_delta_log/`.

---

## 9. Common errors

### `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`

Spark is missing the S3A connector. Add:

```python
extra_packages=["org.apache.hadoop:hadoop-aws:3.4.2"]
```

to `configure_spark_with_delta_pip(...)`.

### `NoSuchBucket` or bucket not found

Create the bucket first:

```bash
aws --endpoint-url http://localhost:9000 s3 mb s3://delta-cdf-demo
```

### Connection refused to `localhost:9000`

Confirm the container is running and ports are mapped:

```bash
docker ps
curl http://localhost:9000
```

### Access denied / signature errors

Check:

- access key / secret key
- endpoint URL
- `path.style.access=true`
- HTTP vs HTTPS setting

### `Unable to load region from any of the providers` (or region/endpoint errors)

Hadoop 3.4.x uses AWS SDK v2, which needs an explicit region for a non-AWS
endpoint. Set:

```python
.config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
```

---

## 10. Summary checklist

- [ ] Start RustFS with `rustfs/rustfs:latest`
- [ ] Expose `9000` for S3 API and `9001` for the console
- [ ] Ensure mounted `data/` and `logs/` are writable by UID/GID `10001:10001`
- [ ] Create bucket `delta-cdf-demo`
- [ ] Add `org.apache.hadoop:hadoop-aws:3.4.2` to Spark packages
- [ ] Configure Spark S3A implementation class, endpoint, region, credentials, path-style access, and SSL flag
- [ ] Replace all local `delta/...` paths with `s3a://delta-cdf-demo/...`
- [ ] Use an S3A checkpoint path for streaming
- [ ] Verify `_delta_log/`, Parquet files, and checkpoint files are created in RustFS

---

## Recommended next code change

Rather than editing `main.py` directly and losing the local demo, the safest
approach is to add a separate `main_rustfs.py` or make `main.py` switch paths
based on environment variables, for example:

```bash
USE_RUSTFS=true \
RUSTFS_ENDPOINT=http://localhost:9000 \
RUSTFS_ACCESS_KEY=rustfsadmin \
RUSTFS_SECRET_KEY=rustfsadmin \
RUSTFS_BUCKET=delta-cdf-demo \
uv run python main.py
```

That keeps local-file mode and RustFS mode both easy to test.
