"""Example code for generating a returns chart and table for MVO backtests."""

import datetime as dt
import polars as pl
import sf_quant.data as sfd
import matplotlib.pyplot as plt
import seaborn as sns

# Parameters
start = dt.date(1996, 1, 1)
end = dt.date(2024, 12, 31)
gamma = 50

signal_names = [
    "raw_idiosyncratic_industry_momentum",
    "vol_adj_idiosyncratic_industry_momentum"
]
print("Loading asset returns...")
returns = (
    sfd.load_assets(
        start=start, end=end, columns=["date", "barrid", "return"], in_universe=True
    )
    .sort("date", "barrid")
    .select(
        "date",
        "barrid",
        # Shift -1 to align today's weight with tomorrow's return
        pl.col("return").truediv(100).shift(-1).over("barrid").alias("forward_return"),
    )
)

for signal_name in signal_names:
    print(f"Processing {signal_name}...")

    # Load MVO weights
    # Ensure this path matches exactly where your runner saved the weights
    weights = pl.read_parquet(f"weights/{signal_name}/{gamma}/*.parquet")

    # Compute portfolio returns
    portfolio_returns = (
        weights.join(other=returns, on=["date", "barrid"], how="left")
        .group_by("date")
        .agg(
            pl.col("forward_return").mul(pl.col("weight")).sum().alias("return")
        )
        .sort("date")
    )

    # Compute cumulative log returns
    cumulative_returns = portfolio_returns.select(
        "date", 
        pl.col("return").log1p().cum_sum().mul(100).alias("cumulative_return")
    )

    plt.figure(figsize=(10, 6))
    
    # Convert to pandas for Seaborn compatibility
    sns.lineplot(
        data=cumulative_returns.to_pandas(), 
        x="date", 
        y="cumulative_return"
    )
    
    # Dynamic title so you know which is which
    plt.title(f"Backtest: {signal_name} (Gamma {gamma})")
    plt.xlabel("")
    plt.ylabel("Cumulative Log Returns (%)")
    
    # Saves as "raw_..._chart.png" and "vol_adj_..._chart.png"
    chart_filename = f"{signal_name}_chart.png"
    plt.savefig(chart_filename)
    plt.close() # Close figure to free memory
    
    print(f" -> Saved chart to {chart_filename}")

    # Create summary table
    summary = portfolio_returns.select(
        pl.col("return").mean().mul(252 * 100).alias("mean_return"),
        pl.col("return").std().mul(pl.lit(252).sqrt() * 100).alias("volatility"),
    ).with_columns(
        pl.col("mean_return").truediv(pl.col("volatility")).alias("sharpe")
    )

    # Print summary
    print(f"Summary for {signal_name}:")
    print(summary)
    print("-" * 30)