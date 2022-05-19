
# columns names created in middle step in the execution
CREATED_COLS = [
    'datehour',
    'domain',
    'country',
    'consents_asked',
    'consents_asked_with_consent',
    'avg_consents_asked_per_user',
    'consents_given',
    'consents_given_with_consent',
    'avg_consents_asked_per_user',
    'pageviews',
    'pageviews_with_consent',
    'avg_pageviews_per_user',
    ]


# dimension columns
DIMENSION_COLS = ['datehour', 'domain', 'country']


# columns names created in middle step in the execution
METRICS_COLS = [
    'pageviews',
    'pageviews_with_consent',
    'consents_asked',
    'consents_asked_with_consent',
    'consents_given',
    'consents_given_with_consent',
    'avg_pageviews_per_user',
    ]
