import sys
import argparse
import time
import parser_football
import gc

def CreateParserArg():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '-number_chunks', '-number', default=5)
    parser.add_argument('-l', '-lenght_chunks', '-lenght', default=500)
    parser.add_argument('-r', '-recursive', '-recursive_call', action="store_const", const=True)
    return parser
def get_console_values():
    parser = CreateParserArg()
    namespace = parser.parse_args(sys.argv[1:])
    return int(namespace.n), int(namespace.l), namespace.r

if __name__ == '__main__':
    numbers_chunks, lenght_chunks, recursive_call = get_console_values()
    if recursive_call:
        while True:
            df = parser_football.get_dataframe(numbers_chunks=numbers_chunks, lenght_chunks=lenght_chunks)

            time_df = time.time()
            df.to_csv('../data/dataset_{id_df}.{format_df}'.format(id_df = time_df, format_df = 'csv'))

            print('Save dataset ../data/dataset_{id_df}.{format_df}'.format(id_df = time_df, format_df = 'csv'))
            gc.collect()
            time.sleep(1200)

    df = parser_football.get_dataframe(numbers_chunks=numbers_chunks, lenght_chunks=lenght_chunks)

    time_df = time.time()
    df.to_csv('../data/dataset_{id_df}.{format_df}'.format(id_df = time_df, format_df = 'csv'))

    print('Save dataset ../data/dataset_{id_df}.{format_df}'.format(id_df = time_df, format_df = 'csv'))

    sys.exit()
