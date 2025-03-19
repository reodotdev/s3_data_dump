from pandas.core.interchange.dataframe_protocol import DataFrame

from utils.common import *
from utils.s3_util import s3Interactions
from utils.db_connections import *


def upload_data(aws_secret, aws_access_key, bucket_name, file_prefix, df:DataFrame):
    s3 = s3Interactions(aws_secret=aws_secret, access_key=aws_access_key)
    s3.dump_data_in_s3(bucket_name=bucket_name, file_prefix=file_prefix, df=df)
    del s3

def fetch_product_usage(
        tenant_id,
        from_date,
        to_date,
        aws_secret,
        aws_access_key,
        bucket_name,
        chunksize=10000,
        org_filter_chunksize = 1000):
    data_len_df = clonedproduct_conn.run_query(
        f"""
        select
          count(1) as tc
        from
          product_usage p
          inner join product_usage_org pu on p.id = pu.product_usage_id
        where
            p.tenant_id = '{tenant_id}'
            and date(p.created_at) >= '{from_date}'::date
            and date(p.created_at) <= '{to_date}'::date
        """, chunk_query=False
    )

    data_len = data_len_df["tc"].to_list()[0]
    print(f"extraction length: {data_len}")
    activities_data = None
    fetch_pg_data_using_copy(
        db=clonedproduct_conn.DATABASE_URL,
        query=f"""
            select
              to_timestamp(p.event_at)::date as date,
              to_char(to_timestamp(p.event_at), 'MM') as month,
              to_char(to_timestamp(p.event_at), 'DD') as day,
              to_char(to_timestamp(p.event_at), 'YYYY') as year,
              p.event_id,
              p.activity_type,
              p.ip_addr as "IP",
              p.product_id,
              p.user_id,
              p.environment,
              p.source,
              p.meta as meta,
              pu.org_id::text as org_id
            from
              product_usage p
              inner join product_usage_org pu on p.id = pu.product_usage_id
            where
              p.tenant_id = '{tenant_id}'
              and date(p.created_at) >= '{from_date}'::date
              and date(p.created_at) <= '{to_date}'::date
            """,
        file_name=f"telemetry_{tenant_id}_{from_date}_and_{to_date}.csv"
    )
    activities_data = pd.read_csv(f"telemetry_{tenant_id}_{from_date}_and_{to_date}.csv")
    if activities_data is None:
        raise ConnectionError(f"""Query to fetch telemetry data didn't execute properly and hasn't returned anything. pls check the query.
Here's the query for reference:
select
    to_timestamp(p.event_at)::date as date,
    to_char(to_timestamp(p.event_at), 'MM') as month,
    to_char(to_timestamp(p.event_at), 'DD') as day,
    to_char(to_timestamp(p.event_at), 'YYYY') as year,
    p.event_id,
    p.activity_type,
    p.ip_addr as "IP",
    p.product_id,
    p.user_id,
    p.environment,
    p.source,
    p.meta as meta,
    pu.org_id::text as org_id
from
    product_usage p
    inner join product_usage_org pu on p.id = pu.product_usage_id
where
    p.tenant_id = '{tenant_id}'
    and date(p.created_at) >= '{from_date}'::date
    and date(p.created_at) <= '{to_date}'::date
    """)
    elif activities_data.shape[0] != data_len:
        raise ValueError(f"""All the data wasn't pulled from db. Pls check the connector.
Data to be pulled: {data_len}. Data pulled: {activities_data.shape[0]}
Here's the query for reference:
select
    to_timestamp(p.event_at)::date as date,
    to_char(to_timestamp(p.event_at), 'MM') as month,
    to_char(to_timestamp(p.event_at), 'DD') as day,
    to_char(to_timestamp(p.event_at), 'YYYY') as year,
    p.event_id,
    p.activity_type,
    p.ip_addr as "IP",
    p.product_id,
    p.user_id,
    p.environment,
    p.source,
    p.meta as meta,
    pu.org_id::text as org_id
from
    product_usage p
    inner join product_usage_org pu on p.id = pu.product_usage_id
where
    p.tenant_id = '{tenant_id}'
    and date(p.created_at) >= '{from_date}'::date
    and date(p.created_at) <= '{to_date}'::date
    """)
    else:
        print("activities data pulled correctly!")
    org_ids = list(set([org for org in activities_data[((activities_data["org_id"].notna()) & (activities_data["org_id"] is not None))]["org_id"].to_list() if org != '']) - set(['no-org', 'NO-ORG', 'NO_ORG']))

    number_of_orgs = len(org_ids)
    org_datas = []
    for index in range(0, number_of_orgs, org_filter_chunksize):
        org_datas.append(
            org_service_conn.run_query(f"""
    select id::text as org_id, company_name, domain, linked_in_url, estimated_num_employees,
        annual_revenue_printed, total_funding, city, state, country, industry, sub_industry, keywords, tech_masala
    from org_entity
    where id in ('{"','".join(org_ids[index:index+org_filter_chunksize])}')
        """, chunk_query=False)
        )
    org_data = pd.concat(org_datas)
    del org_datas
    final_data = activities_data.merge(
        org_data,
        on=["org_id"],
        how="left"
    )
    types = set(final_data["activity_type"].to_list())
    for type in types:
        tmp_df = final_data[final_data["activity_type"]==type]
        upload_data(aws_secret=aws_secret, aws_access_key=aws_access_key, bucket_name=bucket_name, file_prefix=type, df=tmp_df)
    del final_data
    del org_data
    del activities_data
    os.remove(f"telemetry_{tenant_id}_{from_date}_and_{to_date}.csv")

tenant_id=input("Enter tenant id: ")
aws_secret=input("Enter aws secret: ")
aws_access_key=input("Enter aws access key: ")
from_date=input("Enter from date: ")
to_date=input("Enter to date: ")
bucket_name=input("Enter bucket name: ")

fetch_product_usage(
    tenant_id=tenant_id,
    from_date=from_date,
    to_date=to_date,
    aws_secret=aws_secret,
    aws_access_key=aws_access_key,
    bucket_name=bucket_name
)

print("Added data!")





