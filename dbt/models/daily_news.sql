-- news volume and sentiment per ticker per Tokyo calendar day
select
    ticker,
    (published_at at time zone 'Asia/Tokyo')::date as d,
    count(*)                                        as n_articles,
    avg(sentiment)                                  as avg_sentiment
from {{ source('raw', 'raw_news') }}
group by 1, 2
