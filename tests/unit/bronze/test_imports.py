def test_bronze_modules_can_be_imported():
    from spark_apps.bronze.config import minio
    from spark_apps.bronze.config import topics
    from spark_apps.bronze.decoders import avro_decoder
    from spark_apps.bronze.jobs import bronze_topic_job
    from spark_apps.bronze.schemas import topic_schemas
    from spark_apps.bronze.sinks import minio_sink
    from spark_apps.bronze.sources import kafka_source
    from spark_apps.bronze.transforms import timestamp_transform

    assert minio is not None
    assert topics is not None
    assert avro_decoder is not None
    assert bronze_topic_job is not None
    assert topic_schemas is not None
    assert minio_sink is not None
    assert kafka_source is not None
    assert timestamp_transform is not None