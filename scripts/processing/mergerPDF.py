import pandas as pd
import os


def parse_period(period_str):
    start_str, end_str = period_str.split(' - ')
    start_day, start_month = start_str.split()
    end_day, end_month = end_str.split()

    # Преобразуем месяц в число
    month_to_num = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
        'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
        'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
    }

    start_period = f"{start_day.zfill(2)}.{month_to_num[start_month]}"
    end_period = f"{end_day.zfill(2)}.{month_to_num[end_month]}"

    return start_period, end_period

def parse_days(days_str):
    # Создаем битовую маску для 7 дней недели
    bitmask = [0] * 7
    for char in str(days_str):
        if char.isdigit():
            day = int(char)
            if 1 <= day <= 7:
                bitmask[day - 1] = 1

    return bitmask



def process_timetable_files(filename):
    if not os.path.exists(filename):
        print(f"Предупреждение: Файл {filename} не найден, пропускаем")
        return None

    try:
        print(f"Обрабатываем файл: {filename}")

        df = pd.read_csv(filename)

        # Обрабатываем период
        period_data = df['Period'].apply(parse_period)
        df['startPeriod'] = [x[0] if x[0] else '' for x in period_data]
        df['endPeriod'] = [x[1] if x[1] else '' for x in period_data]

        # Обрабатываем дни
        days_data = df['Days'].apply(parse_days)
        df[['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']] = pd.DataFrame(
            days_data.tolist(), index=df.index)

        # Удаляем исходные колонки
        df = df.drop(['Period', 'Days'], axis=1)

        return df

    except Exception as e:
        print(f"Ошибка при обработке файла {filename}: {e}")
        return None


def combine(all_dfs):
    if all_dfs:
        # Объединяем все DataFrame
        combined_df = pd.concat(all_dfs, ignore_index=True)

        desired_order = ['Flight', 'From', 'To', 'startPeriod', 'endPeriod',
                        'Departure', 'Arrival', 'Aircraft', 'Duration',
                        'Monday', 'Tuesday', 'Wednesday', 'Thursday',
                        'Friday', 'Saturday', 'Sunday']

        combined_df = combined_df[desired_order]

        # Сохраняем результат
        output_filename = "output/Skyteam_Timetable_combined.csv"
        combined_df.to_csv(output_filename, index=False)

        # Выводим статистику
        print(f"\n=== ОБРАБОТКА ЗАВЕРШЕНА ===")
        print(f"Объединенный файл: {output_filename}")
        print(f"Всего строк: {len(combined_df):,}")
        print(f"Колонки: {list(combined_df.columns)}")

        return combined_df
    else:
        print("Не удалось обработать ни один файл")
        return None


def main():
    all_dfs = []
    for i in range(1, 36):
        filename = f"output/Skyteam_Timetable_part{i}.csv"
        all_dfs.append(process_timetable_files(filename))

    combine(all_dfs)


if __name__ == "__main__":
    main()
