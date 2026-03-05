import pyarrow.parquet as pq

CURATED = "data/results/curated_taxi_trips"
KPIS = "data/results/daily_zone_kpis"

curated = pq.read_table(CURATED)
kpis = pq.read_table(KPIS)

print("Curated schema:")
print(curated.schema)

print("\nKPIs schema:")
print(kpis.schema)

curated_df = curated.to_pandas()
kpis_df = kpis.to_pandas()

print("\nTop 10 curated by total_amount:")
print(curated_df.nlargest(10, "total_amount"))

print("\nTop 20 KPIs by trip_count:")
print(kpis_df.nlargest(20, "trip_count"))
