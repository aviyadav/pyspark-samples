import pandas as pd
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

# Create a pandas DataFrame
df = pd.DataFrame({"name": ["Alice", "Bob"], "id": [1, 2]})

# Write to a local Delta Table folder
write_deltalake("./tmp/delta-table-rs", df)

# Read the Delta Table back
dt = DeltaTable("./tmp/delta-table-rs")
df_read = dt.to_pandas()
print(df_read)
