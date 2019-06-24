import datetime

from .extract_data import *
from .s3_helper import *


def get_keywords_data(keyword):
    """

    :param keyword:
    :return:
    """
    API_KEY = os.environ['API_KEY']

    try:
        api_url = 'https://newsapi.org/v2/everything?q={}&from=2019-05-24&sortBy=publishedAt&apiKey={}'.format(
            keyword, API_KEY)
        print("calling the api...{}".format(api_url))
        resp = requests.get(api_url)
        if resp.status_code != 200:
            raise Exception('GET {} {} had some exception'.format(api_url, resp.status_code))
        else:
            resp_dict = resp.json()
            print("getting keywords data from api .....")
            return resp_dict

    except Exception as exc:
        print("Something went wrong...{}".format(exc))


def keywords_api_to_s3():
    keywords = ['Tempus+Labs', 'Eric+Lefkofsky', 'Cancer', 'Immunotherapy']
    for keyword in keywords:
        keywords_response = get_keywords_data(keyword)
        keywords_df_data = get_df(keywords_response)
        csv_data = get_csv_buffer(keywords_df_data)
        if csv_data is not None:
            # <s3_bucket>/<source_name>
            s3_location = keyword
            current_dt = datetime.datetime.now()
            write_to_s3(csv_data, s3_location, '{}_top_headlines.csv'.format(current_dt))
