import camelot
import time
import re
import pandas as pd
import concurrent.futures
import os
os.environ['CAMELOT_USE_JPYPE'] = 'true' # Принудительно использовать JPype

# Подготавливает чанки и запускает их в многопотоке с учетом правила: каждая страница чанка должна начинаться с "FROM:"
def prepare_chunks(tables, chunk_size):
    chunks = []
    current_page = 0
    total_pages = len(tables)

    while current_page < total_pages:
        chunk_start = current_page
        chunk_end = min(chunk_start + chunk_size, total_pages)

        # Проверяем последнюю страницу чанка на соответствие правилу
        while chunk_end < total_pages:

            # Проверяем, есть ли на странице таблица с "FROM:"
            table = tables[chunk_end]

            df_ = table.df
            if not df_.empty and str(df_.iloc[0, 0]).strip() == "FROM:":
                break
            chunk_end += 1  # Сдвигаем конец чанка

        # Добавляем чанк
        print(f"    Create a chunk: {chunk_start}-{chunk_end}")
        chunks.append((chunk_start, chunk_end))
        current_page = chunk_end

    return chunks


def clean_row(row):
    cleaned_cells = []
    for cell in row:
        cell_str = str(cell)

        # Если есть кавычки и переносы - разделяем
        if '\n' in cell_str:
            parts = cell_str.split('\n')
            cleaned_cells.extend(parts)
        else:
            cleaned_cells.append(cell_str)

    return cleaned_cells

def parse_chunk(tables, global_from_="", global_to_=""):
    all_data_rows = []

    from_ = global_from_
    to_ = global_to_

    for ind in range(0, len(tables)):
        df_ = tables[ind].df
        num_cols = len(df_.columns)
        if not df_.empty and any("FROM:" in str(cell) for cell in df_.iloc[0]):
            from_ = str(df_.iloc[1, num_cols - 1])
            to_ = str(df_.iloc[0, num_cols - 1])
            if from_ == "":
                # Ищем первую ячейку с тремя заглавными буквами в строке 1
                for col in range(num_cols):
                    cell_value = str(df_.iloc[0, col])
                    if re.fullmatch(r'\s*[A-Z]{3}\s*', cell_value):
                        from_ = cell_value
                        break

            if to_ == "":
                # Ищем первую ячейку с тремя заглавными буквами в строке 0
                for col in range(num_cols):
                    cell_value = str(df_.iloc[1, col])
                    if re.fullmatch(r'\s*[A-Z]{3}\s*', cell_value):
                        to_ = cell_value
                        break

        for i, row in df_.iterrows():
            cleaned_row = clean_row(row)
            # Паттерн для даты: 23 Dec...
            if len(cleaned_row) >= 7 and re.match(r'^\d{1,2}\s+[A-Za-z]{3}', cleaned_row[0]):
                first_record = cleaned_row[0:7]
                first_record.append(from_)
                first_record.append(to_)
                all_data_rows.append(first_record)

            if len(cleaned_row) >= 14 and re.match(r'^\d{1,2}\s+[A-Za-z]{3}', cleaned_row[7]):
                second_record = cleaned_row[7:14]
                second_record.append(to_)
                second_record.append(from_)
                all_data_rows.append(second_record)

    return all_data_rows, from_, to_


def create_csv(all_data_rows, output_csv):
    if all_data_rows:
        start = time.time()
        final_df = pd.DataFrame(all_data_rows)

        column_names = [
            'Period', 'Days', 'Departure', 'Arrival',
            'Flight', 'Aircraft', 'Duration', 'From', 'To'
        ]

        final_df.columns = column_names[:len(final_df.columns)]
        final_df.to_csv(output_csv, index=False, encoding='utf-8')
        end = time.time()
        print(f"    Successfully saved! Total records: {len(final_df)}. (It takes {end - start} seconds)")


def read_chunk(pdf_path, chunk_start__, chunk_end__):
    return camelot.read_pdf(pdf_path, pages=f"{chunk_start__}-{chunk_end__}", flavor='stream', suppress_stdout=True)

def parallel_read_pdf(pdf_path, start_page, total_pages, chunk_size, max_workers=16):
    # Параллельная обработка
    chunks_info = []
    for chunk_start in range(start_page, total_pages + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, total_pages)
        chunks_info.append((chunk_start, chunk_end))
    all_tables = []
    results = [None] * len(chunks_info)

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {}
        for i, (chunk_start, chunk_end) in enumerate(chunks_info):
            future = executor.submit(read_chunk, pdf_path, chunk_start, chunk_end)
            future_to_index[future] = i

        for future in concurrent.futures.as_completed(future_to_index):
            index = future_to_index[future]
            chunk_start, chunk_end = chunks_info[index]
            try:
                print(f"    Read a pages: {chunk_start}-{chunk_end}")
                results[index] = future.result()
            except Exception as err:
                print(f"    Error_1: {err}")

    for result in results:
        if result is not None:
            all_tables.extend(result)
    return all_tables


global_from = ""
global_to = ""
def parse_pdf_camelot(pdf_path, output_csv, start_page, total_pages, chunk_size=100, max_workers_read=16, max_workers_parse=16):
    global global_from, global_to
    all_data_rows = []

    # 0. Открытие PDF
    print(f"Processing from page {start_page} to {total_pages}. Reading...")
    start = time.time()
    tables = parallel_read_pdf(pdf_path, start_page, total_pages, chunk_size, max_workers_read)
    end = time.time()
    print(f"    Reading successfully! Total tables: {len(tables)}. (It takes {end - start} seconds)")


    # 1. Подготавливаем чанки (К сожалению только в одном потоке)
    print(f"Preparing chunks of tables:")
    start = time.time()
    chunks = prepare_chunks(tables, chunk_size)
    end = time.time()
    print(f"    Preparing successfully! Total chunks of tables: {len(chunks)}. (It takes {end - start} seconds)")


    # 2. Парсим каждый
    print(f"Parsing every table...")
    start = time.time()
    with concurrent.futures.ProcessPoolExecutor(max_workers_parse) as executor:
        future_to_file = {executor.submit(parse_chunk, tables[chunk_start:chunk_end], global_from, global_to): (chunk_start, chunk_end) for chunk_start, chunk_end in chunks}

        for future in concurrent.futures.as_completed(future_to_file):
            chunk_start, chunk_end = future_to_file[future]
            try:
                cortege = future.result()
                chunk_data = cortege[0]
                if chunk_end == chunks[-1][1]:
                    global_from = cortege[1]
                    global_to = cortege[2]
                all_data_rows.extend(chunk_data)
                print(f"    Table completed: {chunk_start}-{chunk_end}")
            except Exception as err:
                print(f"    Error_2: {err}")
    end = time.time()
    print(f"    Parsing successfully! Total records: {len(all_data_rows)}. (It takes {end - start} seconds)")

    # 3. Сохраняем результат
    print(f"Saving all data to {output_csv}...")
    create_csv(all_data_rows, output_csv)


def main():
    pdf_file = "Airlines/Skyteam_Timetable.pdf"
    start_pages = 5
    total_pages = 27514
    chunk_size = 50
    output_chunk_size = 800

    file_shift = 0
    # Создаем чанки для выгрузки
    for chunk_num, output_start in enumerate(range(start_pages, total_pages + 1, output_chunk_size), 1):
        output_end = min(output_start + output_chunk_size - 1, total_pages)
        csv_file = f"output/Skyteam_Timetable_part{chunk_num + file_shift}.csv"

        print(f"Processing part {chunk_num}: pages {output_start}-{output_end}")
        parse_pdf_camelot(pdf_file, csv_file, start_page=output_start, total_pages=output_end, chunk_size=chunk_size)


if __name__ == "__main__":
    main()
