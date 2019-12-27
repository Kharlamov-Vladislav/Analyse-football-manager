import time
import parser_football


if __name__ == '__main__':
    df = parser_football.get_dataframe(numbers_chunks=1, lenght_chunks=100)
    df.to_csv('../data/dataset_{id_df}.{format_df}'.format(id_df = time.time(), format_df = 'csv'))
    time.sleep(60)
