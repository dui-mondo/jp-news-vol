-- one row per ticker-day. target_spike = next day's high-low range > 1.5x the trailing 20d average range
with p as (
    select
        ticker, d, close,
        ln(close / nullif(lag(close) over w, 0)) as ret,
        (high - low) / nullif(close, 0)          as range_pct
    from {{ source('raw', 'raw_prices') }}
    window w as (partition by ticker order by d)
),
j as (
    select
        p.*,
        coalesce(n.n_articles, 0)    as news_cnt,
        coalesce(n.avg_sentiment, 0) as news_sent
    from p
    left join {{ ref('daily_news') }} n on n.ticker = p.ticker and n.d = p.d
),
f as (
    select
        *,
        avg(range_pct)    over w20 as range_avg20,
        stddev(ret)       over w5  as vol5,
        stddev(ret)       over w20 as vol20,
        avg(news_cnt)     over w20 as cnt_avg20,
        stddev(news_cnt)  over w20 as cnt_sd20,
        lead(range_pct)   over w   as next_range
    from j
    window
        w   as (partition by ticker order by d),
        w5  as (partition by ticker order by d rows between 4 preceding and current row),
        w20 as (partition by ticker order by d rows between 19 preceding and current row)
)
select
    ticker, d, ret, range_pct, vol5, vol20,
    vol5 / nullif(vol20, 0)                      as vol_ratio,
    range_pct / nullif(range_avg20, 0)           as range_rel,
    news_cnt, news_sent,
    (news_cnt - cnt_avg20) / nullif(cnt_sd20, 0) as news_z,
    case
        when next_range is null then null
        when next_range > 1.5 * range_avg20 then 1
        else 0
    end as target_spike
from f
where vol20 is not null
