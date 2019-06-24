import datetime

from .extract_data import *
from .s3_helper import *


def api_to_s3():
    english_sources = get_sources_from_api()
    for source in english_sources:
        headlines_data = get_top_headlines(source)
        x = get_df(headlines_data)
        csv_data = get_csv_buffer(x)
        if csv_data is not None:
            # <s3_bucket>/<source_name>
            s3_location = source
            current_dt = datetime.datetime.now()
            write_to_s3(csv_data, s3_location, '{}_top_headlines.csv'.format(current_dt))
