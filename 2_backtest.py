"""
Example code for submitting an MVO backtest as a job to the supercomputer.
"""
import os  # Import os to handle paths
from sf_backtester import BacktestConfig, BacktestRunner, SlurmConfig

### config
DRY_RUN = False
PROJECT_ROOT = "/home/connerd4/silverfund/momentum/" # Define this once
GAMMA = 50
### config

slurm_config = SlurmConfig(
    n_cpus=8,
    mem="32G",
    time="03:00:00",
    mail_type="BEGIN,END,FAIL",
    max_concurrent_jobs=30,
)

# -----------------------------------------------------------------------------
# STRATEGY 1: RAW
# -----------------------------------------------------------------------------
backtest_config_raw = BacktestConfig(
    signal_name="raw_idiosyncratic_industry_momentum",
    # FIX: Use os.path.join or the full string to make the path absolute
    data_path=os.path.join(PROJECT_ROOT, "alpha_raw_mom_data.parquet"),
    gamma=GAMMA,
    project_root=PROJECT_ROOT,
    byu_email="connerd4@byu.edu",
    constraints=["ZeroBeta", "ZeroInvestment"],
    slurm=slurm_config,
)

# -----------------------------------------------------------------------------
# STRATEGY 2: VOL-ADJUSTED
# -----------------------------------------------------------------------------
backtest_config_vol_adj = BacktestConfig(
    signal_name="vol_adj_idiosyncratic_industry_momentum",
    # FIX: Use os.path.join or the full string to make the path absolute
    data_path=os.path.join(PROJECT_ROOT, "alpha_vol_adj_mom_data.parquet"),
    gamma=GAMMA,
    project_root=PROJECT_ROOT,
    byu_email="connerd4@byu.edu",
    constraints=["ZeroBeta", "ZeroInvestment"],
    slurm=slurm_config,
)

# -----------------------------------------------------------------------------
# CREATE OUTPUT DIRECTORIES MANUALLY
# -----------------------------------------------------------------------------
# The backtester will fail if these folders don't exist.
# We need: project_root / weights / signal_name / gamma

# 1. For Raw Momentum
output_dir_raw = os.path.join(
    PROJECT_ROOT, 
    "weights", 
    "raw_idiosyncratic_industry_momentum", 
    f"{GAMMA}"
)
os.makedirs(output_dir_raw, exist_ok=True)
print(f"Created directory: {output_dir_raw}")

# 2. For Vol-Adjusted Momentum
output_dir_vol = os.path.join(
    PROJECT_ROOT, 
    "weights", 
    "vol_adj_idiosyncratic_industry_momentum", 
    f"{GAMMA}"
)
os.makedirs(output_dir_vol, exist_ok=True)
print(f"Created directory: {output_dir_vol}")

# -----------------------------------------------------------------------------
# NOW SUBMIT
# -----------------------------------------------------------------------------
runner_raw = BacktestRunner(backtest_config_raw)
runner_raw.submit(dry_run=DRY_RUN)

runner_vol = BacktestRunner(backtest_config_vol_adj)
runner_vol.submit(dry_run=DRY_RUN)