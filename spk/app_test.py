from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from apscheduler.schedulers.blocking import BlockingScheduler

def fetch_data():
    try:
        print("Mengambil data dari database...")
        engine = create_engine('postgresql://owwl:@localhost:5432/postgres')
        dataset = pd.read_sql_table('contracts', con=engine)
        spk_released = pd.read_sql_table('data_spk_released', con=engine)
        print("Data berhasil diambil.")
        print("Data kontrak:")
        print(dataset.head())
        print("Data SPK dirilis:")
        print(spk_released.head())
        return dataset, spk_released
    except Exception as e:
        print("Error saat mengambil data:", e)
        return None, None

def perform_calculations(dataset, spk_released):
    try:
        print("Melakukan perhitungan...")
        today = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))
        dataset['start_date'] = pd.to_datetime(dataset['start_date'])
        dataset['contract_end_date'] = pd.to_datetime(dataset['contract_end_date'])
        spk_released['start_date'] = pd.to_datetime(spk_released['start_date'])
        spk_released['end_date'] = pd.to_datetime(spk_released['end_date'])
        dataset['contract_duration_days'] = (dataset['contract_end_date'] - dataset['start_date']).dt.days
        dataset['contract_duration_months'] = dataset['contract_duration_days'] / 30
        dataset['remaining_duration_days'] = (dataset['contract_end_date'] - today).dt.days.clip(lower=0)
        dataset['remaining_duration_months'] = dataset['remaining_duration_days'] / 30
        dataset['predicted_end_date'] = today + pd.to_timedelta(dataset['remaining_duration_months'] * 30, unit='days')
        dataset.loc[dataset['contract_end_date'] < today, 'predicted_end_date'] = dataset['contract_end_date']
        spk_released['actual_duration_days'] = (today - spk_released['start_date']).dt.days.clip(lower=0)
        spk_released['actual_pace'] = spk_released['physical_progress_value'] / spk_released['actual_duration_days']
        average_actual_pace = spk_released['actual_pace'].mean()
        average_tender_finish_months = dataset['contract_duration_months'].mean()
        average_tender_finish_date = today + pd.to_timedelta(average_tender_finish_months * 30, unit='days')
        spk_accumulation = spk_released.groupby('spk_id')['spk_value'].sum()
        spk_released['spk_duration_days'] = (spk_released['end_date'] - spk_released['start_date']).dt.days
        spk_released['spk_duration_months'] = spk_released['spk_duration_days'] / 30
        spk_monthly_forecast = spk_released.groupby('spk_id').apply(lambda x: x['spk_value'].sum() / x['spk_duration_months'].sum())
        print("Perhitungan selesai.")
        print("Menyimpan data ke dalam tabel predicted_outputs...")
        engine = create_engine('postgresql://owwl:@localhost:5432/postgres')
        output = pd.DataFrame({
            'prediction_date': [today] * len(dataset),
            'contract_id': dataset['contract_id'],
            'predicted_end_date': dataset['predicted_end_date'],
            'average_actual_pace': [average_actual_pace] * len(dataset),
            'average_tender_finish_months': [average_tender_finish_months] * len(dataset),
            'average_tender_finish_date': [average_tender_finish_date] * len(dataset)
        })
        output = output.merge(spk_released[['spk_id', 'actual_duration_days']], how='left', on='spk_id')
        output['spk_accumulation'] = output['spk_id'].map(spk_accumulation)
        output['spk_monthly_forecast'] = output['spk_id'].map(spk_monthly_forecast)
        output.to_sql('predicted_outputs', con=engine, if_exists='append', index=False)
        print("Data berhasil disimpan.")
    except Exception as e:
        print("Error saat melakukan perhitungan:", e)

# Menjalankan fungsi fetch_data dan perform_calculations
dataset, spk_released = fetch_data()
if dataset is not None and spk_released is not None:
    perform_calculations(dataset, spk_released)
