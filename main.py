import sys
import os
import tempfile
import struct
import zipfile
import sqlite3
import pandas as pd
import shutil
from tqdm import tqdm

main_azm = sys.argv[1]
src_dir = sys.argv[2]
out_dir = sys.argv[3]

def azm_compress():
    pass

def main():

    tmp_dir = tempfile.TemporaryDirectory().name

    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)
        
    assert not os.path.isdir(tmp_dir)

    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)
    
    assert os.path.isdir(out_dir)

    main_azm_extract_dir = os.path.join(tmp_dir, "main")
    main_db = os.path.join(main_azm_extract_dir, "azqdata.db")

    with zipfile.ZipFile(main_azm, 'r') as zip_ref:
        zip_ref.extract("azqdata.db", main_azm_extract_dir)

    conn = sqlite3.connect(main_db)
    main_location_df = pd.read_sql("select * from location", conn, parse_dates=["time"]).sort_values(by="time")
    main_commander_location_df = pd.read_sql("select * from commander_location", conn)
    main_indoor_locationn_df = pd.read_sql("select * from indoor_location", conn)
    main_location_df["geom"] = main_location_df.apply(lambda x: lat_lon_to_geom(x["positioning_lat"], x["positioning_lon"]), axis=1)
    thin_location_df = main_location_df[["time", "posid", "geom"]]
    conn.close()

    print("clone gps start")

    num_log = 0
    
    for root, dirs, files in os.walk(src_dir):

        for name in files:
            if not name.endswith("azm"):
                continue
            num_log += 1
    
    max_percent_per_log = 100 / num_log
    
    pbar = tqdm(total=100)
    global prev_progress
    prev_progress = 0

    def update_progress(progress):
        global prev_progress
        new_progress = current_log_index * max_percent_per_log + progress * max_percent_per_log
        new_progress = round(new_progress, 2)
        delta = new_progress - prev_progress
        pbar.update(delta)
        prev_progress = new_progress

    current_log_index = 0

    for root, dirs, files in os.walk(src_dir):

        for name in files:
            if not name.endswith("azm"):
                continue
            clone_gps(root, name, tmp_dir, main_location_df, main_commander_location_df, main_indoor_locationn_df, thin_location_df, out_dir, update_progress)
            current_log_index += 1

    pbar.close()
    print("clone gps done")

    assert os.path.isdir(tmp_dir)
    shutil.rmtree(tmp_dir)
    assert not os.path.isdir(tmp_dir)

def clone_gps(root, name, tmp_dir, main_location_df, main_commander_location_df, main_indoor_locationn_df, thin_location_df, out_dir, update_progress):
    src_azm_extract_dir = os.path.join(tmp_dir, name)
    src_db = os.path.join(src_azm_extract_dir, "azqdata.db")
    

    with zipfile.ZipFile(os.path.join(root, name), 'r') as zip_ref:
        zip_ref.extractall(src_azm_extract_dir)

    conn = sqlite3.connect(src_db)

    log_hash = pd.read_sql("select log_hash from logs where log_hash is not null limit 1", conn).iloc[0,0]
    location_df = main_location_df.copy()
    location_df["log_hash"] = log_hash
    ommander_location_df = main_commander_location_df.copy()
    ommander_location_df["log_hash"] = log_hash
    indoor_locationn_df = main_indoor_locationn_df.copy()
    indoor_locationn_df["log_hash"] = log_hash
    location_df.to_sql("location", conn, if_exists="replace", dtype=get_existing_sqlite_table_col_types_dict("location", conn), index=False, chunksize=1000, method="multi")
    ommander_location_df.to_sql("commander_location", conn, if_exists="replace", dtype=get_existing_sqlite_table_col_types_dict("commander_location", conn), index=False, chunksize=1000, method="multi")
    indoor_locationn_df.to_sql("indoor_location", conn, if_exists="replace", dtype=get_existing_sqlite_table_col_types_dict("indoor_location", conn), index=False, chunksize=1000, method="multi")

    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'",conn)["name"].tolist()
    valid_tables = []
    for table in tables:
        posid_df = pd.read_sql("SELECT name FROM pragma_table_info('{}') WHERE name = 'posid';".format(table),conn)
        if len(posid_df) > 0:
            valid_tables.append(table)

    current_table_index = 0
    valid_table_number = len(valid_tables) + 10

    for valid_table in valid_tables:
        current_table_index += 1
        update_progress(current_table_index/valid_table_number)

        if valid_table == "location":
            continue
        df = pd.read_sql("select * from {}".format(valid_table), conn, parse_dates=["time"]).sort_values(by="time").drop(columns=["posid", "geom"])
        df = pd.merge_asof(df, thin_location_df, left_on="time", right_on="time", direction="backward", allow_exact_matches=True)
        update_sql = f"UPDATE {valid_table} SET posid = ?, geom = ? WHERE seqid = ?"
        values_list = [
            (row["posid"], safe_bytes(row["geom"]), row["seqid"])
            for _, row in df.iterrows()
        ]
        
        try:
            conn.execute("BEGIN TRANSACTION")
            cursor = conn.cursor()
            cursor.executemany(update_sql, values_list)
            conn.commit()
            # print(f"Table {valid_table} Updated {len(values_list)} rows successfully.")
        except Exception as e:
            conn.rollback()
            # print("Error during update:", e)

    conn.close()
    
    out_azm_path = os.path.join(out_dir, name)
    if os.path.isfile(out_azm_path):
        os.remove(out_azm_path)
    shutil.make_archive(out_azm_path, 'zip', src_azm_extract_dir)
    os.rename(out_azm_path+".zip", out_azm_path)
    update_progress(1)

def lat_lon_to_geom(lat, lon):
        if lat is None or lon is None:
            return None
        geomBlob = bytearray(60)
        geomBlob[0] = 0
        geomBlob[1] = 1
        geomBlob[2] = 0xe6
        geomBlob[3] = 0x10
        geomBlob[4] = 0
        geomBlob[5] = 0
        bx = bytearray(struct.pack("d", lon)) 
        by = bytearray(struct.pack("d", lat)) 
        geomBlob[6:6+8] = bx
        geomBlob[14:14+8] = by 
        geomBlob[22:22+8] = bx
        geomBlob[30:30+8] = by
        geomBlob[38] = 0x7c
        geomBlob[39] = 1
        geomBlob[40] = 0
        geomBlob[41] = 0
        geomBlob[42] = 0
        geomBlob[43:43+8] = bx
        geomBlob[51:51+8] = by 
        geomBlob[59] = 0xfe
        return geomBlob

def get_existing_sqlite_table_col_types_dict(table, dbcon):
    existing_table_col_types_df = pd.read_sql("PRAGMA table_info({});".format(table), dbcon)[['name', 'type']]
    existing_table_col_types_dict = {}
    for _, row in existing_table_col_types_df.iterrows():
        existing_table_col_types_dict[row['name']] = row['type']
    return existing_table_col_types_dict

def safe_bytes(val):
    try:
        return bytes(val)
    except (TypeError, ValueError):
        return bytes()
    
main()