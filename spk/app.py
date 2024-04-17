from datetime import datetime
import pandas as pd
import numpy as np

# Membaca data dari file CSV
dataset = pd.read_csv('dataset.csv')
spk_released = pd.read_csv('Data spk released.csv')

# Mengubah tipe data kolom tanggal menjadi datetime
dataset['start_date'] = pd.to_datetime(dataset['start_date'])
dataset['contract_end_date'] = pd.to_datetime(dataset['contract_end_date'])
spk_released['start_date'] = pd.to_datetime(spk_released['start_date'])
spk_released['end_date'] = pd.to_datetime(spk_released['end_date'])

# Mendapatkan tanggal hari ini
today = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))

# Menghitung durasi kontrak dalam hari dan bulan
dataset['contract_duration_days'] = (dataset['contract_end_date'] - dataset['start_date']).dt.days
dataset['contract_duration_months'] = dataset['contract_duration_days'] / 30

# Menghitung durasi kontrak yang tersisa dalam hari dan bulan
dataset['remaining_duration_days'] = (dataset['contract_end_date'] - today).dt.days.clip(lower=0)
dataset['remaining_duration_months'] = dataset['remaining_duration_days'] / 30

# Menghitung Prediksi Tanggal Berakhir Kontrak
dataset['predicted_end_date'] = today + pd.to_timedelta(dataset['remaining_duration_months'] * 30, unit='days')
dataset.loc[dataset['contract_end_date'] < today, 'predicted_end_date'] = dataset['contract_end_date']

# Menghitung Average Actual Pace (per hari)
spk_released['actual_duration_days'] = (today - spk_released['start_date']).dt.days.clip(lower=0)
spk_released['actual_pace'] = spk_released['physical_progress_value'] / spk_released['actual_duration_days']
average_actual_pace = spk_released['actual_pace'].mean()

# Menghitung Average Tender Finish (dalam bulan dan tanggal)
average_tender_finish_months = dataset['contract_duration_months'].mean()
average_tender_finish_date = today + pd.to_timedelta(average_tender_finish_months * 30, unit='days')

# Menghitung Akumulasi Aktual per SPK ID
spk_accumulation = spk_released.groupby('spk_id')['spk_value'].sum()

# Menghitung Prediksi Bulanan untuk SPK
spk_released['spk_duration_days'] = (spk_released['end_date'] - spk_released['start_date']).dt.days
spk_released['spk_duration_months'] = spk_released['spk_duration_days'] / 30
spk_monthly_forecast = spk_released.groupby('spk_id').apply(lambda x: x['spk_value'].sum() / x['spk_duration_months'].sum())

# Menampilkan hasil
print("\nRata-Rata Durasi Tender dari Awal hingga Akhir (dalam bulan):")
print(dataset.groupby('contract_id')['contract_duration_months'].mean())
print("\nAverage Actual Pace (per day):", average_actual_pace)
print("Average Tender Finish (in months):", average_tender_finish_months)
print("\nTanggal Mulai Tender untuk Setiap Kontrak:")
print(dataset[['contract_id', 'start_date']])
print("\nAkumulasi Aktual per SPK ID:")
print(spk_accumulation)
print("\nPrediksi Bulanan untuk SPK:")
print(spk_monthly_forecast)
print("\nPrediksi Tanggal Berakhir Kontrak:")
print(dataset[['contract_id', 'predicted_end_date']])
