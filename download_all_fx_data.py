import csv
import os
import multiprocessing

from histdata.api import download_hist_data, Platform

def mkdir_p(path):
    import errno
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
def handle_download(row):
    currency_pair_name, pair, history_first_trading_month = row
    year = int(history_first_trading_month[0:4])
    print('[START] for currency', currency_pair_name)
    output_folder = os.path.join('output', pair)
    mkdir_p(output_folder)
    try:
        while True:
            could_download_full_year = False
            try:
                print('-', download_hist_data(year=year,
                                                pair=pair,
                                                output_directory=output_folder,
                                                verbose=False))
                could_download_full_year = True
            except AssertionError:
                pass  # lets download it month by month.
            month = 1
            while not could_download_full_year and month <= 12:
                print('-', download_hist_data(year=str(year),
                                                month=str(month),
                                                pair=pair,
                                                output_directory=output_folder,
                                                platform=Platform.META_TRADER,
                                                verbose=False))
                month += 1
            year += 1
    except Exception:
        print('[DONE] for currency', currency_pair_name)

def download_all():
    with open('pairs.csv', 'r') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader, None)  # skip the headers

        # Network should be the bottleneck
        nr_parallel_jobs = os.cpu_count() * 2


        pool = multiprocessing.Pool(nr_parallel_jobs)
        rows = [row for row in reader]
        
        pool.map(handle_download, rows)
            


if __name__ == '__main__':
    download_all()
