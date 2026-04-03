"""
stage03_transform_Rucu.py
(EDIT YOUR COPY OF THIS FILE)

Source: validated JSON object
Sink: Polars DataFrame

Purpose

  Transform validated JSON data into a structured format.

Analytical Questions

- Which fields are needed from the JSON data?
- How can records be normalized into tabular form?
- What derived fields would support analysis?

Notes

Following our process, do NOT edit this _case file directly,
keep it as a working example.

In your custom project, copy this _case.py file and
append with _yourname.py instead.

Then edit your copied Python file to:
- extract the fields needed for your analysis,
- normalize records into a consistent structure,
- create any derived fields required.
"""

# ============================================================
# Section 1. Setup and Imports
# ============================================================

import logging
from typing import Any

import polars as pl

# ============================================================
# Section 2. Define Run Transform Function
# ============================================================


def run_transform(
    json_data: list[dict[str, Any]],
    LOG: logging.Logger,
) -> pl.DataFrame:
    """Transform JSON into a structured DataFrame.

    Args:
        json_data (list[dict[str, Any]]): Validated JSON data.
        LOG (logging.Logger): The logger instance.

    Returns:
        pl.DataFrame: The transformed dataset.
    """
    LOG.info("========================")
    LOG.info("STAGE 03: TRANSFORM starting...")
    LOG.info("========================")

    records: list[dict[str, Any]] = []

    for record in json_data:
        records.append(
            {
                "Cat_breed": record["breed"],
                "country": record["country"],
                "origin": record["origin"],
                "coat": record["coat"],
                "pattern": record["pattern"],
            }
        )

    df: pl.DataFrame = pl.DataFrame(records)

    # Derived fields
    df = df.with_columns(
        [
            pl.when(
                pl.col("origin")
                .str.to_lowercase()
                .is_in(["natural", "natural/standard"])
            )
            .then(pl.lit("low-maintenance"))
            .when(pl.col("origin").str.to_lowercase() == "mutation")
            .then(pl.lit("average-maintenance"))
            .when(
                (pl.col("origin").is_null())
                | (pl.col("origin").str.to_lowercase() == "")
            )
            .then(pl.lit("origin-unknown"))
            .otherwise(pl.lit("high-maintenance"))
            .alias("Cat_maintenance_level")
        ]
    )

    LOG.info("Transformation complete.")
    LOG.info(f"DataFrame preview:\n{df.head()}")
    LOG.info("Sink: Polars DataFrame created")

    # Return the transformed DataFrame for use in the next stage.
    return df
