import pandas as pd

from dags import challenge as c


class TestTransformations():

    def test_get_df(self):
        api_data = {
            'status': 'ok',
            'totalResults': 10,
            'articles': [
                {
                    'source': {
                        'id': 'abc-news',
                        'name': 'ABC News'
                    },
                    'author': 'The Associated Press',
                    'title': 'Bikers gather for emotional ceremony following deadly crash',
                    'description': 'Get breaking national and world news',
                    'url': 'https://abcnews.go.com',
                    'urlToImage': 'https://s.abcnews.com/images/x.jpg',
                    'publishedAt': '2019-06-23T19:25:11Z',
                    'content': 'A long-planned Blessing of the Bikes ceremony for motorcycle enthusiasts became a grief-filled '

                }
            ]
        }

        data = [['The Associated Press', 'Bikers gather for emotional ceremony following deadly crash',
                 'Get breaking national and world news', 'https://abcnews.go.com', 'https://s.abcnews.com/images/x.jpg',
                 '2019-06-23T19:25:11Z',
                 'A long-planned Blessing of the Bikes ceremony for motorcycle enthusiasts became a grief-filled ',
                 'ABC News', 'abc-news']]
        expectedDF = pd.DataFrame(data, columns=['author', 'title', 'description', 'url', 'urlToImage',
                                                 'publishedAt', 'content', 'source_name', 'source_id'], index=[0])

        transformedDf = c.get_df(api_data)

        assert transformedDf.equals(expectedDF)
        pd.testing.assert_frame_equal(transformedDf, expectedDF)
