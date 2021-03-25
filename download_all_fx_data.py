import csv
import os
import multiprocessing
from zipfile import ZipFile
import pandas

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

def extract_zip_to_folder(path):
    from pathlib import Path
    folder_name = ".".join(Path(path).name.split('.')[:-1])
    parent_folder = Path(path).parent.absolute()

    extract_folder = Path.joinpath(parent_folder, folder_name)
    os.makedirs(extract_folder.absolute())

    with ZipFile(path, 'r') as zip:
        zip.extractall(path=extract_folder)
        return extract_folder

def handle_download(row):
    currency_pair_name, pair, history_first_trading_month = row
    year = int(history_first_trading_month[0:4])
    print('[START] for currency', currency_pair_name)
    output_folder = os.path.join('output', pair)

    if os.path.exists(output_folder):
        import shutil
        shutil.rmtree(output_folder)

    mkdir_p(output_folder)

    try:
        while True:
            could_download_full_year = False
            path = None
            try:
                path = download_hist_data(year=year,
                                                pair=pair,
                                                output_directory=output_folder,
                                                platform=Platform.META_TRADER,
                                                verbose=False)
                print('-', path)
                extract_zip_to_folder(path)
                could_download_full_year = True
            except AssertionError:
                pass  # lets download it month by month.
            month = 1
            while not could_download_full_year and month <= 12:
                path = download_hist_data(year=str(year),
                                                month=str(month),
                                                pair=pair,
                                                output_directory=output_folder,
                                                platform=Platform.META_TRADER,
                                                verbose=False)
                print('-', path)
                extract_zip_to_folder(path)
                month += 1
            year += 1
            os.remove(path)
    except Exception as e:
        import glob
        # combines all csvs
        extension = 'csv'
        all_filenames = [i for i in glob.glob('{}/**/*.{}'.format(output_folder, extension))]

        file_name = "{}.csv".format(output_folder)
        if os.path.exists(file_name):
            os.remove(file_name)

        combined_csv = open(file_name, 'a')

        for file in all_filenames:
            f = open(file)
            for line in f:
                combined_csv.write(line)

        import shutil
        shutil.rmtree(output_folder)

        print('[DONE] for currency', currency_pair_name)

def download_all():
    with open('pairs.csv', 'r') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader, None)  # skip the headers

        nr_parallel_jobs = os.cpu_count()


        pool = multiprocessing.Pool(nr_parallel_jobs)
        rows = [row for row in reader]

        pool.map(handle_download, rows)
            


if __name__ == '__main__':
    import time
    start_time = time.time()

    download_all()

    print("Took %s seconds to download, extract and join" % (time.time() - start_time))
