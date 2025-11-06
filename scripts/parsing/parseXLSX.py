# Работа с .xlsx
import concurrent.futures
import pandas as pd
import os
import glob

def get_xlsx_files(folder_path):
    # Поиск всех .xlsx файлов в соответствующей папке
    xlsx_files = glob.glob(folder_path + "/*.xlsx")
    return xlsx_files


# Извлекает нужные ячейки с одной страницы
def extract_cells_from_sheet(df_):

    cells = []
    gender_cell = df_.iloc[2, 0]   # A3
    if gender_cell == 'MR':
        cells.append('Male')
    else:
        cells.append('Female')

    name_cell = df_.iloc[2, 1]   # B3
    first_name, last_name = str(name_cell).split(' ', 1)
    cells.append(first_name)
    cells.append(last_name)

    cells.extend([
        df_.iloc[2, 5],   # F3
        df_.iloc[2, 7],   # H3
        df_.iloc[4, 0],   # A5
        df_.iloc[4, 3],   # D5
        df_.iloc[4, 7],   # H5
        df_.iloc[6, 3],   # D7
        df_.iloc[6, 7],   # H7
        df_.iloc[8, 0],   # A9
        df_.iloc[8, 2],   # C9
        df_.iloc[12, 1],  # B13
        df_.iloc[12, 4]   # E13
    ])
    return cells


# Создание столбцов и запись в *.csv
def create_csv(all_data, file_path):
    if all_data:
        columns = [
            'PassengerSex', 'PassengerFirstName', 'PassengerLastName', 'CardNumber', 'Class', 'FlightNumber', 'FromCity',
            'ToCity', 'From', 'To', 'Date', 'Time', 'BookingCode', 'E-TICKET'
        ]

        df_result = pd.DataFrame(all_data, columns=columns)

        filename = os.path.basename(file_path)
        date_part = filename.replace('YourBoardingPassDotAero-', '').replace('.xlsx', '')
        output_filename = f"output/your_boarding_pass_dot_aero_{date_part}.csv"
        df_result.to_csv(output_filename, encoding='utf-8')
    else:
        print("No data found to save")


# Функция на поток
def parsing_thread(file_path):
    all_data = []

    xl_file = pd.ExcelFile(file_path)

    for sheet_name in xl_file.sheet_names:
        df_ = xl_file.parse(sheet_name, header=None)
        cells = extract_cells_from_sheet(df_)
        all_data.append(cells)

    xl_file.close()
    create_csv(all_data, file_path)
    return file_path


def main():
    folder_path = "Airlines/YourBoardingPassDotAero"
    xlsx_files = get_xlsx_files(folder_path)

    # Используем ProcessPoolExecutor для многопоточности
    with concurrent.futures.ProcessPoolExecutor(max_workers=32) as executor:
        # Запускаем обработку всех файлов параллельно
        future_to_file = {executor.submit(parsing_thread, file_path): file_path for file_path in xlsx_files}

        # Собираем результаты по мере завершения
        for future in concurrent.futures.as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                future.result()
                print(f"File completed: {file_path}")
            except Exception as err:
                print(f"Error: {err}")


if __name__ == "__main__":
    main()
