from __future__ import annotations
from typing import Any, Dict, Iterable, Optional
from pymongo import MongoClient, UpdateOne

def get_client(mongo_uri: str) -> MongoClient:
    return MongoClient(mongo_uri)

def get_collection(mongo_uri: str, db_name: str, collection_name: str):
    client = get_client(mongo_uri)
    return client[db_name][collection_name]

def bulk_upsert_by_key(collection, docs: Iterable[Dict[str, Any]], key_field: str) -> int:
    ops = []
    for d in docs:
        if key_field not in d:
            continue
        ops.append(UpdateOne({key_field: d[key_field]}, {"$set": d}, upsert=True))
    if not ops:
        return 0
    res = collection.bulk_write(ops, ordered=False)
    return int(res.upserted_count + res.modified_count + res.matched_count)
