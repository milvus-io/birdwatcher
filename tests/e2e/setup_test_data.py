#!/usr/bin/env python3
"""
Setup test data for birdwatcher E2E tests.
Creates collections, partitions, indexes, and inserts data.
"""

import time
import sys
from pymilvus import MilvusClient, DataType
import numpy as np


def wait_for_milvus(client, max_retries=30):
    """Wait for Milvus to be ready."""
    for i in range(max_retries):
        try:
            client.list_databases()
            print(f"Milvus is ready after {i + 1} attempts")
            return True
        except Exception as e:
            print(f"Waiting for Milvus... attempt {i + 1}/{max_retries}: {e}")
            time.sleep(2)
    return False


def main():
    print("Setting up test data for birdwatcher E2E tests...")

    # Connect to Milvus
    client = MilvusClient(uri="http://localhost:19530")

    # Wait for Milvus to be ready
    if not wait_for_milvus(client):
        print("ERROR: Milvus is not ready after maximum retries")
        sys.exit(1)

    # List existing databases
    databases = client.list_databases()
    print(f"Existing databases: {databases}")

    # Use default database for testing
    client.using_database("default")

    # Drop existing test collections if they exist
    existing_collections = client.list_collections()
    print(f"Existing collections: {existing_collections}")

    for coll_name in ["test_collection", "simple_collection"]:
        if coll_name in existing_collections:
            print(f"Dropping existing collection: {coll_name}")
            client.drop_collection(coll_name)

    # Create test collection with various field types
    print("Creating test_collection...")
    schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=128)
    schema.add_field("name", DataType.VARCHAR, max_length=256)
    schema.add_field("score", DataType.FLOAT)

    client.create_collection(
        collection_name="test_collection",
        schema=schema,
    )
    print("test_collection created")

    # Create partitions
    print("Creating partitions...")
    for part in ["part_a", "part_b"]:
        try:
            client.create_partition("test_collection", part)
            print(f"  Partition '{part}' created")
        except Exception as e:
            print(f"  Partition '{part}' may already exist: {e}")

    # Insert test data into part_a
    print("Inserting data into part_a...")
    np.random.seed(42)
    data = [
        {
            "id": i,
            "embedding": np.random.rand(128).tolist(),
            "name": f"entity_{i}",
            "score": float(i * 0.1)
        }
        for i in range(1000)
    ]
    client.insert("test_collection", data, partition_name="part_a")
    print("  Inserted 1000 records into part_a")

    # Insert test data into part_b
    print("Inserting data into part_b...")
    data2 = [
        {
            "id": i + 1000,
            "embedding": np.random.rand(128).tolist(),
            "name": f"entity_{i + 1000}",
            "score": float(i * 0.2)
        }
        for i in range(500)
    ]
    client.insert("test_collection", data2, partition_name="part_b")
    print("  Inserted 500 records into part_b")

    # Create index
    print("Creating index on embedding field...")
    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name="embedding",
        index_type="IVF_FLAT",
        metric_type="L2",
        params={"nlist": 128}
    )
    client.create_index("test_collection", index_params)
    print("  Index created")

    # Load collection
    print("Loading test_collection...")
    client.load_collection("test_collection")
    print("  Collection loaded")

    # Create a simpler second collection
    print("Creating simple_collection...")
    schema2 = client.create_schema(auto_id=True, enable_dynamic_field=False)
    schema2.add_field("id", DataType.INT64, is_primary=True)
    schema2.add_field("vec", DataType.FLOAT_VECTOR, dim=64)

    client.create_collection(
        collection_name="simple_collection",
        schema=schema2,
    )

    # Insert some data
    data3 = [{"vec": np.random.rand(64).tolist()} for _ in range(100)]
    client.insert("simple_collection", data3)
    print("  simple_collection created with 100 records")

    # Flush to ensure data is persisted
    print("Flushing collections...")
    client.flush("test_collection")
    client.flush("simple_collection")
    print("  Flush completed")

    # Wait a bit for data to be fully persisted
    time.sleep(2)

    # Print summary
    print("\n" + "=" * 50)
    print("Test data setup complete!")
    print("=" * 50)
    print(f"  Databases: {client.list_databases()}")
    print(f"  Collections: {client.list_collections()}")

    # Get collection stats
    stats = client.get_collection_stats("test_collection")
    print(f"  test_collection stats: {stats}")

    print("\nReady for birdwatcher E2E tests!")


if __name__ == "__main__":
    main()
