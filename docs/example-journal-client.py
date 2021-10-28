import pprint


def process_objects(all_objects):
    """Worker function handling incoming objects"""
    for (object_type, objects) in all_objects.items():
        for object_ in objects:
            print(f"New {object_type} object:")
            pprint.pprint(object_)
            print()


def main():
    from swh.journal.client import get_journal_client

    # Usually read from a config file:
    config = {
        "brokers": ["localhost:9092"],
        "group_id": "my-consumer-group",
        "auto_offset_reset": "earliest",
    }

    # Initialize the client
    client = get_journal_client(
        "kafka", object_types=["revision", "release"], privileged=True, **config
    )

    try:
        # Run the client forever
        client.process(process_objects)
    except KeyboardInterrupt:
        print("Called Ctrl-C, exiting.")
        exit(0)


if __name__ == "__main__":
    main()
