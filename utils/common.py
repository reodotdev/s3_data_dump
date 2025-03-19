import os
import pandas as pd
import subprocess

def remove_non_empty_folder(folder_path):
    for root, dirs, files in os.walk(folder_path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
    for name in dirs:
        os.rmdir(os.path.join(root, name))


def process_chunked_query(db, query, tenant_id, current_date, chunksize):
    temp_folder = f"report_{tenant_id}_{current_date}"
    if temp_folder not in os.listdir():
        os.mkdir(temp_folder)
    db.run_query(
        sql_query=query, chunk_query=True, folder=temp_folder, chunksize=chunksize
    )
    fragmented_data = [pd.read_csv(f"{temp_folder}/{file}") for file in os.listdir(temp_folder) if file.endswith(".csv")]
    result = pd.concat(fragmented_data)
    del fragmented_data
    remove_non_empty_folder(temp_folder)
    return result



def fetch_pg_data_using_copy(db, query, file_name):
    command = [
        "psql",
        db,
        "-c", f"""\\copy ({query}) TO '{file_name}' WITH CSV HEADER"""
    ]
    subprocess.run(command)
    print("data pull complete")