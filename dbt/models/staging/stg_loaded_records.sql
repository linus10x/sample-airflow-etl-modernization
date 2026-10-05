{# When a vendor re-sends a corrected file for a day, the newest loaded version wins. #}
with latest_file as (
    select
        checksum,
        feed,
        file_date,
        path,
        processed_at,
        row_number() over (
            partition by feed, file_date
            order by processed_at desc, checksum
        ) as version_rank
    from {{ source('ops', 'file_manifest') }}
    where status = 'loaded'
)

select
    r.feed,
    f.path as file_path,
    f.checksum as file_checksum,
    f.file_date,
    f.processed_at as loaded_at,
    r.ordinal,
    r.payload
from {{ source('raw', 'records') }} as r
inner join latest_file as f on f.checksum = r.checksum
where f.version_rank = 1
