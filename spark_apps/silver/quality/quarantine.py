from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def prepare_quarantine_records(
    df: DataFrame,
    entity_name: str,
) -> DataFrame:
    """
    Enrich invalid records with quarantine metadata.
    """
    return (
        df
        .withColumn(
            "_dq_entity",
            F.lit(entity_name),
        )
        .withColumn(
            "_dq_quarantined_at",
            F.current_timestamp(),
        )
    )


def write_quarantine(
    df: DataFrame,
    table_name: str,
) -> None:
    """
    Append invalid records to an Iceberg
    quarantine table.
    """
    if df.limit(1).count() == 0:
        return

    (
        df.writeTo(
            table_name
        )
        .using(
            "iceberg"
        )
        .createOrReplace()
    )
