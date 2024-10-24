# %%
import pandas as pd
import numpy as np

# Assuming your data is in a DataFrame called 'df'
df = pd.DataFrame(
    {
        "household_id": [37, 37, 37, 241, 242, 155789, 155789, 155789],
        "person": [1, 2, 3, 1, 1, 1, 2, 3],
        "age": [20, 19, 19, 50, 29, 58, 61, 15],
        "income": [10000, 5300, 4700, 90000, 20000, 5000, 110000, np.nan],
        "female": [False, True, False, True, False, False, True, False],
    }
)

# Aggregate to household level
household_df = (
    df.groupby("household_id")
    .agg(
        {
            "person": "count",
            "age": "mean",
            "age": "min",
            "age": "max",
            "income": "mean",
            "income": "sum",
            "female": "sum",
        }
    )
    .rename(
        columns={
            "person": "household_size",
            "age": "mean_age",
            "age": "min_age",
            "age": "max_age",
            "income": "mean_income",
            "income": "total_income",
            "female": "num_female",
        }
    )
)


# Define a function to check if the highest earner is female
def highest_earner_female(group):
    max_income_row = group.loc[group["income"].idxmax()]
    return max_income_row["female"]


# Aggregate to household level
household_df = df.groupby("household_id").agg(
    {
        "person": "count",
        "age": ["mean", "min", "max"],
        "income": ["mean", "sum"],
        "female": "sum",
    }
)

household_df.columns = [
    "household_size",
    "mean_age",
    "min_age",
    "max_age",
    "mean_income",
    "total_income",
    "num_female",
]

# Add the highest_earner_female column
household_df["highest_earner_female"] = df.groupby("household_id").apply(
    highest_earner_female
)

# # Reset the index to make household_id a column
household_df = household_df.reset_index()

household_df

# %%
import polars as pl

# Create the DataFrame
pl_df = pl.DataFrame(
    {
        "household_id": [37, 37, 37, 241, 242, 155789, 155789, 155789],
        "person": [1, 2, 3, 1, 1, 1, 2, 3],
        "age": [20, 19, 19, 50, 29, 58, 61, 15],
        "income": [10000, 5300, 4700, 90000, 20000, 5000, 110000, None],
        "female": [False, True, False, True, False, False, True, False],
    }
)

# Aggregate to household level
pl_household_df = (
    pl_df.group_by("household_id")
    .agg(
        [
            pl.count("person").alias("household_size"),
            pl.mean("age").alias("mean_age"),
            pl.min("age").alias("min_age"),
            pl.max("age").alias("max_age"),
            pl.mean("income").alias("mean_income"),
            pl.sum("income").alias("total_income"),
            pl.sum("female").alias("num_female"),
            pl.col("female")
            .filter(pl.col("income") == pl.col("income").max())
            .last()
            .alias("highest_earner_female"),
        ]
    )
    .sort("household_id")
)

pl_household_df
