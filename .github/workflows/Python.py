import snowflake.connector
import pandas as pd
import hashlib
import os

# ===== Snowflake Connection =====
def get_connection():
    conn = snowflake.connector.connect(
        user='monopoly22',
        password='8638569740picklU',
        account='CMJSUGN-RH40514',
        warehouse='COMPUTE_WH',
        database='CICD',
        schema='CICD_SCHEMA'
    )
    return conn


# ===== Create Hash Key for Each Row =====
def create_hash(df):
    df["hash_key"] = df.astype(str).apply(lambda x:
                     hashlib.md5('|'.join(x).encode()).hexdigest(), axis=1)
    return df


# ===== Read Table =====
def read_table(conn, table_name):
    query = f"select * from {table_name}"
    df = pd.read_sql(query, conn)
    return df


# ===== Validation Logic =====
def validate(source, target):

    print("Source Count:", len(source))
    print("Target Count:", len(target))

    if len(source) != len(target):
        print("❌ Row count mismatch")
    else:
        print("✅ Row count matched")

    source = create_hash(source)
    target = create_hash(target)

    merged = source.merge(target,
                          on="hash_key",
                          how="outer",
                          indicator=True)

    mismatch = merged[merged["_merge"] != "both"]

    if len(mismatch) == 0:
        print("✅ Data Matched Successfully")
        return 0
    else:
        print("❌ Data Mismatch Found")
        mismatch.to_csv("mismatch_report.csv", index=False)
        return 1


# ===== Main =====
if __name__ == "__main__":

    conn = get_connection()

    source_table = "users"
    target_table = "USER_COPY"

    src_df = read_table(conn, source_table)
    tgt_df = read_table(conn, target_table)

    result = validate(src_df, tgt_df)

    exit(result)

