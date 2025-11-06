import pandas as pd


def mergeTicketPointz():
    # Загружаем файлы
    df1 = pd.read_csv('output/ticket.csv', index_col=0, low_memory=False, dtype={'TicketNumber': 'str'})
    df2 = pd.read_csv('output/pointz_aggregator.csv', index_col=0, low_memory=False)

    # Приводим вторую таблицу к виду первой
    df2_transformed = df2.rename(columns={
        'card_number': 'card_number',
        'activity_code': 'FlightNumber',
        'activity_date': 'FlightDate',
        'activity_departure': 'From',
        'activity_arrival': 'To',
        'firstname': 'PassengerFirstName',
        'lastname': 'PassengerLastName'
    })
    df2_transformed = df2_transformed[['card_number', 'FlightNumber', 'FlightDate', 'From', 'To', 'PassengerFirstName', 'PassengerLastName']]

    # Приводим From к верхнему регистру
    df2_transformed['From'] = df2_transformed['From'].str.upper()

    # Создаем ключ для слияния в обоих DataFrame
    for df in [df1, df2_transformed]:
        df['merge_key'] = (
            df['card_number'].str.strip().str.lower() + '_' +
            df['FlightNumber'].str.lower() + '_' +
            df['FlightDate'].astype(str) + '_' +
            df['From'].str.lower()
        )

    # Находим записи из df2, которых нет в df1
    existing_keys = set(df1['merge_key'])
    new_records = df2_transformed[~df2_transformed['merge_key'].isin(existing_keys)].copy()

    # Объединяем существующие записи с новыми
    df_final = pd.concat([df1, new_records], ignore_index=True)

    # Удаляем временный столбец с ключом
    df_final = df_final.drop('merge_key', axis=1)

    # Сохраняем результат
    df_final.to_csv('output/ticket_pointz.csv', index=True)

    print("Слияние завершено!")
    print(f"Исходное количество записей в df1: {len(df1)}")
    print(f"Добавлено новых записей: {len(new_records)}")
    print(f"Итоговое количество записей: {len(df_final)}")


def mergeTicketSkyteam():
    df_merged = pd.read_csv('output/ticket_pointz.csv', index_col=0, low_memory=False, dtype={'TicketNumber': 'str'})
    df3 = pd.read_csv('output/skyteam_exchange__combined.csv', index_col=0, low_memory=False)
    df3_clean = df3[df3['id2'] != 'id2'].copy()

    # Приводим третий файл к структуре df_merged
    df3_transformed = df3.rename(columns={
        'id1': 'FlightNumber',
        'id2': 'card_number',
        'date': 'FlightDate',
        'FROM': 'From',
        'TO': 'To',
        'CLASS': 'Class',
    })
    df3_transformed = df3_transformed[['card_number', 'FlightNumber', 'FlightDate', 'From', 'To', 'Class']]

    # Приводим к верхнему регистру для сравнения
    df3_transformed['From'] = df3_transformed['From'].str.upper()

    # Создаем ключ для слияния в обоих DataFrame
    for df in [df_merged, df3_transformed]:
        df['merge_key'] = (
                df['card_number'].str.strip().str.lower() + '_' +
                df['FlightNumber'].str.lower() + '_' +
                df['FlightDate'].astype(str) + '_' +
                df['From'].str.lower()
        )

    # Находим записи из df3, которых нет в df_merged
    existing_keys = set(df_merged['merge_key'])
    new_records = df3_transformed[~df3_transformed['merge_key'].isin(existing_keys)].copy()

    # Объединяем
    df_final = pd.concat([df_merged, new_records], ignore_index=False)

    # Удаляем временный ключ
    df_final = df_final.drop('merge_key', axis=1)

    # Сохраняем результат
    df_final.to_csv('output/ticket_pointz_skyteam.csv', index=True)

    print("Слияние с третьим файлом завершено!")
    print(f"Добавлено новых записей: {len(new_records)}")
    print(f"Итоговое количество записей: {len(df_final)}")


if __name__ == "__main__":
    mergeTicketPointz()
    mergeTicketSkyteam()
