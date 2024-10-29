import pandas as pd
import os


def read_csv_from_subdirs(folder_path, sleep_sum_path):
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for subdir in dirnames:
            subdir_path = os.path.join(dirpath, subdir)
            # Lista os arquivos dentro do subdiretório
            for filename in os.listdir(subdir_path):
                if filename.endswith('.csv'):
                    csv_file_path = os.path.join(subdir_path, filename)
                    
                    # Condicional para arquivos raw_data
                    if 'raw_data' in filename:
                        df_raw = pd.read_csv(csv_file_path)
                        print(f"Lendo arquivo raw_data: {csv_file_path}")
                        #print(df_raw.head())  # Visualiza as primeiras linhas
                        
                    # Condicional para arquivos activity
                    elif 'activity_index' in filename:
                        df_activity_index = pd.read_csv(csv_file_path)
                        print(f"Lendo arquivo activity: {csv_file_path}")
                        #print(df_activity_index.head())  # Visualiza as primeiras linhas

                    elif 'wear_detection' in filename:
                        df_wear_detection = pd.read_csv(csv_file_path)
                        print(f"Lendo arquivo wear_detection: {csv_file_path}")
                        #print(df_wear_detection.head())  # Visualiza as primeiras linhas

                    elif 'ck_predictions' in filename:
                        df_ck_predictions = pd.read_csv(csv_file_path)
                        print(f"Lendo arquivo ck_predictions: {csv_file_path}")
                        #print(df_ck_predictions.head())  # Visualiza as primeiras linhas

            # chamar a funcao para formatar aqui
            df_raw, df_activity_index, df_wear_detection, df_ck_predictions = df_format(df_raw, df_activity_index, df_wear_detection, df_ck_predictions)

            a = os.path.splitext(os.path.basename(csv_file_path))
            patient_id_csv = a[0].split('_')
            patient_id_csv[0]
            
            df_final = merge_data(sleep_sum_path, patient_id_csv[0], df_raw, df_activity_index)
            df_final.to_csv(folder_path + f"/{patient_id_csv[0]}_data_final.csv", index=False)

            # print(f"\n\nTabela: {patient_id_csv[0]}")
            # print(df_final.head()) # gerar um csv
        break  # Interrompe após o primeiro nível de diretórios
    return 



def df_format (df_raw, df_activity_index, df_wear_detection, df_ck_predictions):

    df_ck_predictions = df_ck_predictions.rename(columns={'Time': 'Datetime'})
    df_ck_predictions["Datetime"] = pd.to_datetime(df_ck_predictions["Datetime"])

    df_ck_predictions['Date'] = df_ck_predictions["Datetime"].dt.date
    df_ck_predictions['Time'] = df_ck_predictions["Datetime"].dt.strftime('%H:%M:%S')

    df_ck_predictions = df_ck_predictions[['Date', 'Time', 'sleep_predictions']]


    df_activity_index = df_activity_index.rename(columns={'Time': 'Datetime'})
    df_activity_index["Datetime"] = pd.to_datetime(df_activity_index["Datetime"])

    df_activity_index['Date'] = df_activity_index["Datetime"].dt.date
    df_activity_index['Time'] = df_activity_index["Datetime"].dt.strftime('%H:%M:%S')

    df_activity_index = df_activity_index[['Date', 'Time', 'activity_index']]

    
    df_raw["Unnamed: 0"] = pd.to_datetime(df_raw["Unnamed: 0"])
    df_raw['Date'] = df_raw["Unnamed: 0"].dt.date
    df_raw['Time'] = df_raw["Unnamed: 0"].dt.strftime('%H:%M:%S')

    df_raw = df_raw.drop(columns=["Unnamed: 0"])
    df_raw = df_raw[['Date', 'Time', 'X', 'Y', 'Z', 'T', 'Sleep']]

    return df_raw, df_activity_index, df_wear_detection, df_ck_predictions


def merge_data (sleep_sum_path, patient_id_csv, df_raw, df_activity_index):
    # Reading sleep sum file
    sleep_sum = pd.read_csv(sleep_sum_path)
    sleep_sum_cut = sleep_sum[['ID', 'calendar_date', 'sleeponset_ts', 'wakeup_ts']]
    sleep_sum_cut.calendar_date = pd.to_datetime(sleep_sum_cut.calendar_date, dayfirst=True).dt.date
    sleep_sum_cut.sleeponset_ts = pd.to_datetime(sleep_sum_cut.sleeponset_ts).dt.time
    sleep_sum_cut.wakeup_ts = pd.to_datetime(sleep_sum_cut.wakeup_ts).dt.time
    
    patient_sleep_sum = sleep_sum_cut[sleep_sum_cut['ID'] == patient_id_csv]
    patient_sleep_sum.sort_values(by=['calendar_date'], inplace=True)


    for _, row in patient_sleep_sum.iterrows():
        calendar_date = row['calendar_date']
        sleeponset_ts = row['sleeponset_ts']
        wakeup_ts = row['wakeup_ts']

        mask = df_raw['Date'] == calendar_date
        date_filtered_df = df_raw[mask]

        date_filtered_df['Time'] = pd.to_datetime(date_filtered_df['Time']).dt.time

        # Encontrar o indice 
        sleeponset_idx = date_filtered_df[date_filtered_df['Time'] == sleeponset_ts].index
        wakeup_idx = date_filtered_df[date_filtered_df['Time'] == wakeup_ts].index

        if not sleeponset_idx.empty and not wakeup_idx.empty:
            df_raw.loc[sleeponset_idx[0]:wakeup_idx[0], 'Sleep'] = 1


    merged_df = pd.merge(df_raw, df_activity_index, how='left', on=['Date', 'Time'])

    # Propagar os valores de activity_index
    merged_df['activity_index'] = merged_df['activity_index'].fillna(method='ffill')

    merged_df = merged_df[merged_df['Date'].isin(patient_sleep_sum.calendar_date)]
    
    return merged_df


################# testes

def read_csv_from_subdirs_teste(folder_path, sleep_sum_path):
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for subdir in dirnames:
            subdir_path = os.path.join(dirpath, subdir)
            # Lista os arquivos dentro do subdiretório
            for filename in os.listdir(subdir_path):
                if filename.endswith('.csv'):
                    csv_file_path = os.path.join(subdir_path, filename)
                    
                    # Condicional para arquivos raw_data
                    if 'raw_data' in filename:
                        df_raw = pd.read_csv(csv_file_path, nrows=1000)
                        print(f"Lendo arquivo raw_data: {csv_file_path}")
                        #print(df_raw.head())  # Visualiza as primeiras linhas
                        
                    # Condicional para arquivos activity
                    elif 'activity_index' in filename:
                        df_activity_index = pd.read_csv(csv_file_path, nrows=1000)
                        print(f"Lendo arquivo activity: {csv_file_path}")
                        #print(df_activity_index.head())  # Visualiza as primeiras linhas

                    elif 'wear_detection' in filename:
                        df_wear_detection = pd.read_csv(csv_file_path, nrows=1000)
                        print(f"Lendo arquivo wear_detection: {csv_file_path}")
                        #print(df_wear_detection.head())  # Visualiza as primeiras linhas

                    elif 'ck_predictions' in filename:
                        df_ck_predictions = pd.read_csv(csv_file_path, nrows=1000)
                        print(f"Lendo arquivo ck_predictions: {csv_file_path}")
                        #print(df_ck_predictions.head())  # Visualiza as primeiras linhas

            # chamar a funcao para formatar aqui
            print(df_raw.head())
            df_raw, df_activity_index, df_wear_detection, df_ck_predictions = df_format(df_raw, df_activity_index, df_wear_detection, df_ck_predictions)

            a = os.path.splitext(os.path.basename(csv_file_path))
            patient_id_csv = a[0].split('_')
            patient_id_csv[0]
            
            df_final = merge_data(sleep_sum_path, patient_id_csv[0], df_raw, df_activity_index)
            #teste_final(sleep_sum_path, patient_id_csv[0], df_raw, df_activity_index)
            #df_final.to_csv(folder_path + f"/{patient_id_csv[0]}_data_final.csv", index=False)

            #print(f"\n\nTabela: {patient_id_csv[0]}")
            print(df_final.head()) # gerar um csv
        break  # Interrompe após o primeiro nível de diretórios
    return

# folder_path = "C:/Users/marim/Documents/Faculdade/TCC/patient_act_data_CSV"
# sleep_sum_path = 'C:/Users/marim/Documents/Faculdade/TCC/sleepandsummary_total_long_pos_meia_noite.csv'
# read_csv_from_subdirs(folder_path, sleep_sum_path)
