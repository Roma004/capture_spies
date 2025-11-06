import pandas as pd
import matplotlib.pyplot as plt


def mergeTicketPointz():
    # Загружаем файлы
    df1 = pd.read_csv('../Skyteam_Timetable_combined.csv', index_col=0, low_memory=False)
    df1['key'] = df1['From'].str.upper() + '_' + df1['To'].str.upper()
    result_df = df1.groupby('key', as_index=False).size()
    result_df.columns = ['ticket', 'count']

    result_df = result_df.sort_values('count', ascending=False).head(20)

    plt.figure(figsize = (12,6))
    plt.bar(result_df['ticket'].astype(str), result_df['count'])
    plt.xlabel('Рейс')
    plt.ylabel('Количество перелётов')
    plt.title('Самые популярные рейсы')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('flight_counts.png')

    plt.show()


if __name__ == '__main__':
    mergeTicketPointz()
