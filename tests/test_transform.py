import pandas as pd
from utils.transform import clean_dataframe

def test_clean_dataframe_removes_bad_dates():
    df = pd.DataFrame({
        'start_date': ['2024-01-01', 'not a date'],
        'end_date': ['2024-02-01', '2024-03-01']
    })
    clean_df = clean_dataframe(df, date_columns=['start_date', 'end_date'])
    assert pd.isna(clean_df.loc[1, 'start_date'])
    assert clean_df.loc[0, 'start_date'] == pd.Timestamp("2024-01-01")

