import pandas as pd

def detect_real_type(series):
    """
    Определяет реальный тип данных в серии, игнорируя строковые значения
    """
    # Сначала проверяем, есть ли числовые значения
    numeric_values = pd.to_numeric(series, errors='coerce')

    # Считаем, сколько значений успешно преобразовалось в числа
    successful_conversions = numeric_values.notna().sum()

    if successful_conversions > 0:
        # Если есть успешные преобразования, проверяем тип
        if (numeric_values.dropna() % 1 == 0).all():  # Все числа целые
            return 'int'
        else:
            return 'float'
    else:
        return 'string'

def analyze_csv_simple(file_path, delimiter=','):
    """
    Простой анализ CSV файла: проверка на NaN, уникальность и тип данных
    """
    df = pd.read_csv(file_path, delimiter=delimiter, low_memory=False)

    print(f"Анализ файла: {file_path}")
    print(f"Строк: {len(df)}, Столбцов: {len(df.columns)}")
    print("-" * 60)

    for column in df.columns:
        dtype = detect_real_type(df[column])

        # Проверяем наличие пропущенных значений (NaN, 'Not presented', etc.)
        missing_indicators = ['Not presented', 'not presented', 'NOT PRESENTED', '', 'NaN', 'nan', 'None', 'null', 'Null', 'NULL', 'N/A']
        has_missing = df[column].isna().any() or df[column].astype(str).isin(missing_indicators).any()
        # Кол-во не NaN
        non_missing_count = len(df[column]) - df[column].isna().sum() - df[column].astype(str).isin(missing_indicators).sum()

        # Проверяем уникальность
        unique_count = df[column].nunique() - has_missing
        is_unique = unique_count == non_missing_count

        # Выводим информацию
        print(f"Столбец: {column}")
        print(f"  Тип данных: {dtype}")
        print(f"  Может быть NaN: {'Да' if has_missing else 'Нет'}")
        print(f"  Все значения уникальны: {'Да' if is_unique else 'Нет'}, Кол-во уникальных: {unique_count}/{len(df[column])}")
        print()


# Пример использования
if __name__ == "__main__":
    analyze_csv_simple("output/ticket.csv", delimiter=',')
