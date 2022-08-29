import sys
import os
import tempfile
import struct
import zipfile
import sqlite3
import pandas as pd
import shutil
import ray
import msgpack
import setproctitle
import psutil

import logging
from progress_bar import ProgressBar
from ray.dashboard import *

ray.init()

elm_table_main_col_types = {
    "log_hash": "BIGINT",
    "time": "DATETIME",
    "modem_time": "DATETIME",
    "seqid": "INT",
    "posid": "INT",
    "netid": "INT",
    "geom": "BLOB",
    "event_id": "smallint",
    "msg_id": "INT",
    "name": "TEXT",
    "symbol": "TEXT",
    "protocol": "TEXT",    
    "info": "TEXT",
    "detail": "TEXT",
    "detail_hex": "TEXT",
    "detail_str": "TEXT",
}

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

    args = []
    print("clone gps start")
    
    num_ticks = 100
    num_log = 0
    
    for root, dirs, files in os.walk(src_dir):

        for name in files:
            if not name.endswith("azm"):
                continue
            num_log += 1

    pb = ProgressBar(num_ticks, num_log)
    actor = pb.actor

    for root, dirs, files in os.walk(src_dir):

        for name in files:
            if not name.endswith("azm"):
                continue
            args.append(clone_gps.remote(root, name, tmp_dir, main_location_df, main_commander_location_df, main_indoor_locationn_df, thin_location_df, actor, out_dir))
    
    
    pb.print_until_done()
    ray.get(args)
    
    print("clone gps done")

    assert os.path.isdir(tmp_dir)
    shutil.rmtree(tmp_dir)
    assert not os.path.isdir(tmp_dir)

@ray.remote
def clone_gps(root, name, tmp_dir, main_location_df, main_commander_location_df, main_indoor_locationn_df, thin_location_df, actor, out_dir):
    src_azm_extract_dir = os.path.join(tmp_dir, name)
    src_db = os.path.join(src_azm_extract_dir, "azqdata.db")

    with zipfile.ZipFile(os.path.join(root, name), 'r') as zip_ref:
        zip_ref.extractall(src_azm_extract_dir)

    conn = sqlite3.connect(src_db)

    log_hash = pd.read_sql("select log_hash from logs where log_hash is not null limit 1", conn).iloc[0,0]
    location_df = main_location_df.copy()
    location_df["log_hash"] = log_hash
    location_df.to_sql("location", conn, if_exists="replace", dtype=elm_table_main_col_types, index=False)
    main_commander_location_df.to_sql("commander_location", conn, if_exists="replace", dtype=elm_table_main_col_types, index=False)
    main_indoor_locationn_df.to_sql("indoor_location", conn, if_exists="replace", dtype=elm_table_main_col_types, index=False)

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
        actor.update.remote(name, current_table_index/valid_table_number * 100)
        if valid_table in ["location", "commander_location", "indoor_location"]:
            continue
        df = pd.read_sql("select * from {}".format(valid_table), conn, parse_dates=["time"]).sort_values(by="time").drop(columns=["posid", "geom"])
        df = pd.merge_asof(df, thin_location_df, left_on="time", right_on="time", direction="backward", allow_exact_matches=True)
        df.to_sql(valid_table, conn, if_exists="replace", dtype=elm_table_main_col_types, index=False, chunksize=10, method="multi")

    conn.close()
    
    out_azm_path = os.path.join(out_dir, name)
    if os.path.isfile(out_azm_path):
        os.remove(out_azm_path)
    shutil.make_archive(out_azm_path, 'zip', src_azm_extract_dir)
    os.rename(out_azm_path+".zip", out_azm_path)
    
    actor.update.remote(name, 100)

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

main()