import pandas as pd


def combine_skyteam_files():
    start_year, start_month = 2017, 1
    end_year, end_month = 2018, 1

    all_files = []
    current_year, current_month = start_year, start_month

    # Проходим по всем месяцам включая январь 2018
    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        filename = f"output/skyteam_exchange_{current_year}-{current_month:02d}.csv"
        all_files.append(filename)

        # Переходим к следующему месяцу
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

    # Читаем и объединяем все файлы
    dfs = []
    for file in all_files:
        try:
            df = pd.read_csv(file)

            df = df.iloc[:, 1:]

            dfs.append(df)
            print(f"Обработан файл: {file}")
        except FileNotFoundError:
            print(f"Файл {file} не найден, пропускаем")
            continue

    # Объединяем все DataFrame
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=False)

        combined_df.to_csv('output/skyteam_exchange__combined.csv')
        print(f"Объединенный файл создан. Всего записей: {len(combined_df)}")
        print(f"Обработано файлов: {len(dfs)}")
    else:
        print("Не найдено ни одного файла для обработки")


def main():
    combine_skyteam_files()

if __name__ == "__main__":
    main()
