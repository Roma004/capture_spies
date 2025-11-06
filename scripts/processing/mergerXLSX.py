import pandas as pd
from datetime import datetime, timedelta


def combine_boarding_passes():
    # Создаем список всех файлов за указанный период
    start_date = datetime(2017, 1, 1)
    end_date = datetime(2018, 1, 1)

    all_files = []
    current_date = start_date
    while current_date < end_date:
        filename = f"output/your_boarding_pass_dot_aero_{current_date.strftime('%Y-%m-%d')}.csv"
        all_files.append(filename)
        current_date += timedelta(days=1)

    # Читаем и объединяем все файлы
    dfs = []
    for file in all_files:
        try:
            df = pd.read_csv(file, index_col=0)
            dfs.append(df)
            print(f"Обработан файл: {file}")
        except FileNotFoundError:
            print(f"Файл {file} не найден, пропускаем")
            continue

    # Объединяем все DataFrame
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=False)

        combined_df.to_csv('output/your_boarding_pass_dot_aero__combined.csv')
        print(f"Объединенный файл создан. Всего записей: {len(combined_df)}")
    else:
        print("Не найдено ни одного файла для обработки")


def main():
    combine_boarding_passes()

if __name__ == "__main__":
    main()
