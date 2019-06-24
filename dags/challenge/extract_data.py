from io import StringIO

import pandas as pd
import requests

API_KEY = '411d2105b0ce44c79c199affeb3ee852'


def get_sources_from_api(language='en'):
    """

    :param language: Defaulted to English sources can be reused for any other languages
    :return: List of sources for the english sources
    """
    try:
        api_url = 'https://newsapi.org/v2/sources?language={}&apiKey={}'.format(language, API_KEY)
        print("calling the api...{}".format(api_url))
        resp = requests.get(api_url)
        if resp.status_code != 200:
            raise Exception('GET get_sources_from_api hit an exception{}'.format(resp.status_code))
        else:
            resp_dict = resp.json()
            print("getting sources data from api .....")
            sources = resp_dict['sources']
            source_list = [i['id'] for i in sources]
            return source_list

    except Exception as exc:
        print("Something went wrong...{}".format(exc))


def get_top_headlines(source):
    """

    :param sources: The source for which the headline needs to be fetched.
    :return:
    """
    api_url = 'https://newsapi.org/v2/top-headlines?sources={}&apiKey={}'.format(source, API_KEY)
    print("calling the api...{}".format(api_url))
    resp = requests.get(api_url)
    if resp.status_code != 200:
        raise Exception('GET get_top_headlines {}'.format(resp.status_code))
    else:

        resp_dict = resp.json()
        print("Returning data from api .....")
        return resp_dict


def get_headlines_for_sources_generator(sources):
    """
     This method takes source as an arguement and yields the json response.
    :param sources:
    :return: yields a json object for each source
    """

    for i in sources:
        print(i)
        print("Getting json list")
        resp = get_top_headlines(i)
        yield resp


def get_headlines_for_sources(sources, num_of_sources):
    """

    :param sources:
    :param num_of_sources:
    :return: json object list of data
    """
    data = []
    x = get_headlines_for_sources_generator(sources)
    for i in range(num_of_sources):
        print(i)
        data.append(next(x))
    return data


def get_df(data):
    """

    :param data: A dictionary type data input that is received from the json response
    :return: a pandas dataframe consisting of all the articles.
    """
    dfObj = pd.DataFrame()
    for articles_dict in data['articles']:
        my_dict = articles_dict
        my_dict['source_name'] = articles_dict.get('source').get('name')
        my_dict['source_id'] = articles_dict.get('source').get('id')
        del my_dict['source']
        data = pd.DataFrame(my_dict, index=[0])
        dfObj = dfObj.append(data, ignore_index=True)
    return dfObj


def get_csv_buffer(df):
    """

    :param df: a pandas df to be converted as list
    :return: a csv file
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()
