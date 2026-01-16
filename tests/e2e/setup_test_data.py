#!/usr/bin/env python3
"""
Setup test data for birdwatcher E2E tests.
Creates comprehensive test data to ensure all birdwatcher commands have output.
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


def create_test_database(client):
    """Create additional test database."""
    print("\n=== Creating test database ===")
    try:
        # Create a test database
        client.create_database("test_db")
        print("  Created database: test_db")
    except Exception as e:
        print(f"  Database creation skipped (may already exist): {e}")


def create_main_collection(client):
    """Create main test collection with comprehensive data."""
    print("\n=== Creating main test collection ===")

    # Drop if exists
    if "test_collection" in client.list_collections():
        print("  Dropping existing test_collection")
        client.drop_collection("test_collection")

    # Create schema with multiple field types
    schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=128)
    schema.add_field("name", DataType.VARCHAR, max_length=256)
    schema.add_field("score", DataType.FLOAT)
    schema.add_field("count", DataType.INT32)
    schema.add_field("active", DataType.BOOL)

    client.create_collection(
        collection_name="test_collection",
        schema=schema,
    )
    print("  Created test_collection with 6 fields")

    # Create multiple partitions
    partitions = ["part_a", "part_b", "part_c"]
    for part in partitions:
        try:
            client.create_partition("test_collection", part)
            print(f"  Created partition: {part}")
        except Exception as e:
            print(f"  Partition {part} may exist: {e}")

    # Insert data in batches to create multiple segments
    np.random.seed(42)
    total_records = 0

    for batch in range(3):
        for part in partitions:
            batch_size = 500
            data = [
                {
                    "id": batch * 10000 + partitions.index(part) * 1000 + i,
                    "embedding": np.random.rand(128).tolist(),
                    "name": f"entity_{batch}_{part}_{i}",
                    "score": float(np.random.rand()),
                    "count": int(np.random.randint(0, 1000)),
                    "active": bool(np.random.choice([True, False])),
                }
                for i in range(batch_size)
            ]
            client.insert("test_collection", data, partition_name=part)
            total_records += batch_size

        # Flush after each batch to create segments
        client.flush("test_collection")
        print(f"  Batch {batch + 1}: Inserted and flushed {batch_size * len(partitions)} records")

    print(f"  Total records inserted: {total_records}")

    # Create index
    print("  Creating IVF_FLAT index...")
    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name="embedding",
        index_type="IVF_FLAT",
        metric_type="L2",
        params={"nlist": 128}
    )
    client.create_index("test_collection", index_params)

    # Load collection to generate replica info
    print("  Loading collection...")
    client.load_collection("test_collection")

    # Create alias
    print("  Creating aliases...")
    try:
        client.create_alias("test_collection", "test_alias")
        client.create_alias("test_collection", "test_alias_2")
        print("  Created aliases: test_alias, test_alias_2")
    except Exception as e:
        print(f"  Alias creation note: {e}")

    return "test_collection"


def create_additional_collections(client):
    """Create additional collections for more comprehensive testing."""
    print("\n=== Creating additional collections ===")

    collections_created = []

    # Simple collection (auto_id, no dynamic field)
    if "simple_collection" in client.list_collections():
        client.drop_collection("simple_collection")

    schema = client.create_schema(auto_id=True, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=64)

    client.create_collection("simple_collection", schema=schema)

    # Insert data
    data = [{"vec": np.random.rand(64).tolist()} for _ in range(200)]
    client.insert("simple_collection", data)
    client.flush("simple_collection")
    print("  Created simple_collection with 200 records")
    collections_created.append("simple_collection")

    # Binary vector collection
    if "binary_collection" in client.list_collections():
        client.drop_collection("binary_collection")

    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("binary_vec", DataType.BINARY_VECTOR, dim=128)

    client.create_collection("binary_collection", schema=schema)

    # Insert binary vectors
    data = [
        {
            "id": i,
            "binary_vec": np.random.bytes(16),  # 128 bits = 16 bytes
        }
        for i in range(100)
    ]
    client.insert("binary_collection", data)
    client.flush("binary_collection")
    print("  Created binary_collection with 100 records")
    collections_created.append("binary_collection")

    # Sparse vector collection (if supported)
    try:
        if "sparse_collection" in client.list_collections():
            client.drop_collection("sparse_collection")

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("sparse_vec", DataType.SPARSE_FLOAT_VECTOR)

        client.create_collection("sparse_collection", schema=schema)

        # Insert sparse vectors
        data = [
            {
                "id": i,
                "sparse_vec": {j: float(np.random.rand()) for j in np.random.choice(1000, size=10, replace=False)},
            }
            for i in range(50)
        ]
        client.insert("sparse_collection", data)
        client.flush("sparse_collection")
        print("  Created sparse_collection with 50 records")
        collections_created.append("sparse_collection")
    except Exception as e:
        print(f"  Sparse collection skipped (may not be supported): {e}")

    return collections_created


def trigger_compaction(client, collection_name):
    """Trigger compaction to generate compaction task data."""
    print("\n=== Triggering compaction ===")
    try:
        # Get collection ID first
        collection_info = client.describe_collection(collection_name)
        print(f"  Collection: {collection_name}")

        # Compact the collection
        client.compact(collection_name)
        print("  Compaction triggered")

        # Wait a bit for compaction to be registered
        time.sleep(2)
    except Exception as e:
        print(f"  Compaction note: {e}")


def perform_searches(client, collection_name):
    """Perform search queries to generate query stats."""
    print("\n=== Performing search queries ===")
    search_vectors = [np.random.rand(128).tolist() for _ in range(10)]

    for i, vec in enumerate(search_vectors):
        try:
            results = client.search(
                collection_name=collection_name,
                data=[vec],
                anns_field="embedding",
                limit=10,
                output_fields=["name", "score"]
            )
            print(f"  Search {i+1}: {len(results[0])} results")
        except Exception as e:
            print(f"  Search {i+1} failed: {e}")

    # Also do some queries
    try:
        results = client.query(
            collection_name=collection_name,
            filter="count > 500",
            limit=100,
            output_fields=["id", "name", "count"]
        )
        print(f"  Query with filter: {len(results)} results")
    except Exception as e:
        print(f"  Query failed: {e}")


def create_users_and_roles(client):
    """Create test users and roles."""
    print("\n=== Creating users and roles ===")

    try:
        # Note: User management requires root credentials in production
        # In test environment, this may or may not work depending on auth settings
        client.create_user("test_user", "Test123!")
        print("  Created user: test_user")
    except Exception as e:
        print(f"  User creation note: {e}")

    try:
        client.create_role("test_role")
        print("  Created role: test_role")
    except Exception as e:
        print(f"  Role creation note: {e}")


def print_summary(client):
    """Print summary of created test data."""
    print("\n" + "=" * 60)
    print("Test Data Setup Complete!")
    print("=" * 60)

    print(f"\nDatabases: {client.list_databases()}")
    print(f"Collections: {client.list_collections()}")

    for coll in client.list_collections():
        try:
            stats = client.get_collection_stats(coll)
            desc = client.describe_collection(coll)
            print(f"\n{coll}:")
            print(f"  Row count: {stats.get('row_count', 'N/A')}")
            print(f"  Fields: {len(desc.get('fields', []))}")
        except Exception as e:
            print(f"  {coll}: Could not get stats - {e}")

    print("\n" + "=" * 60)
    print("Ready for birdwatcher E2E tests!")
    print("=" * 60)


def main():
    print("Setting up comprehensive test data for birdwatcher E2E tests...")
    print("=" * 60)

    # Connect to Milvus
    client = MilvusClient(uri="http://localhost:19530")

    # Wait for Milvus to be ready
    if not wait_for_milvus(client):
        print("ERROR: Milvus is not ready after maximum retries")
        sys.exit(1)

    # Use default database
    client.using_database("default")

    # Create test database
    create_test_database(client)

    # Create main test collection with comprehensive data
    main_collection = create_main_collection(client)

    # Create additional collections
    create_additional_collections(client)

    # Trigger compaction
    trigger_compaction(client, main_collection)

    # Perform searches
    perform_searches(client, main_collection)

    # Create users and roles (may fail depending on auth settings)
    create_users_and_roles(client)

    # Wait for all operations to settle
    print("\nWaiting for operations to settle...")
    time.sleep(3)

    # Print summary
    print_summary(client)


if __name__ == "__main__":
    main()
