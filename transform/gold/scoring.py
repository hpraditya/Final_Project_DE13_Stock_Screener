import pandas as pd


THRESHOLDS = {
    "roe": 0.15,           # >= 15%
    "der": 0.5,            # <= 0.5
    "fcf": 0,              # > 0
    "eps_growth_yoy": 0.1, # >= 10%
    "per": 15,             # <= 15
    "pbv": 1.5,            # <= 1.5
    "graham_combined": 22.5,  # PER x PBV <= 22.5
}

TOTAL_CRITERIA = len(THRESHOLDS)


def score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    criteria = [
        df["roe"] >= THRESHOLDS["roe"],
        df["der"] <= THRESHOLDS["der"],
        df["fcf"] > THRESHOLDS["fcf"],
        df["eps_growth_yoy"] >= THRESHOLDS["eps_growth_yoy"],
        df["per"] <= THRESHOLDS["per"],
        df["pbv"] <= THRESHOLDS["pbv"],
        (df["per"] * df["pbv"]) <= THRESHOLDS["graham_combined"],
    ]
    df["criteria_passed"] = sum(c.astype(int) for c in criteria)
    df["status"] = df["criteria_passed"].apply(
        lambda x: "LOLOS" if x == TOTAL_CRITERIA else "TIDAK"
    )
    return df
