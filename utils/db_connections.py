import pandas as pd
from sqlalchemy import create_engine, text

class PostGresConn:
    def __init__(self, database_url):
        # Database connection
        self.DATABASE_URL = database_url
        self.engine = create_engine(self.DATABASE_URL)

    def run_query(self, sql_query, folder="", chunk_query=True, chunksize=10000):
        if chunk_query:
            # Use a context manager to handle the connection
            index = 0
            with self.engine.connect().execution_options(
                    stream_results=True,
                    max_row_buffer=chunksize
            ) as connection:
                chunks = pd.read_sql(sql_query, connection, chunksize=chunksize)
                index=1
                for chunk in chunks:
                    chunk.to_csv(f'{folder}/{index}.csv', header=True, index=False)
                    index+=1
            return index
        else:
            # Use a context manager to handle the connection
            with self.engine.connect() as connection:
                result = connection.execute(text(sql_query))

                # Fetch data into DataFrame
                res_df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return res_df



org_service_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@prod-org-service.cluster-c6herkpwuh2p.us-east-1.rds.amazonaws.com/org_service")
git_activity_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@github-activity.cluster-ro-c6herkpwuh2p.us-east-1.rds.amazonaws.com/github_activity")
prod_activity_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@product-activity-instance-1.c6herkpwuh2p.us-east-1.rds.amazonaws.com:5432/product_activity")
doc_activity_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@activity-db.cluster-ro-c6herkpwuh2p.us-east-1.rds.amazonaws.com:5432/document_activity")
tenant_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@activity-db-instance-1.c6herkpwuh2p.us-east-1.rds.amazonaws.com/tenant")
dev_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@dev-graph.cluster-c6herkpwuh2p.us-east-1.rds.amazonaws.com/dgraph")
segment_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@developer-service.cluster-c6herkpwuh2p.us-east-1.rds.amazonaws.com/segment")
ip_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@prod-ip-service.cluster-c6herkpwuh2p.us-east-1.rds.amazonaws.com/ip_service")
apiary_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@prod-feedback.cluster-c6herkpwuh2p.us-east-1.rds.amazonaws.com/apiary")
notification_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@developer-service.cluster-c6herkpwuh2p.us-east-1.rds.amazonaws.com/notification")
flatdata_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@flatdata.cluster-c6herkpwuh2p.us-east-1.rds.amazonaws.com/flatdata")
ah_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@ah-database-prod.cluster-ro-c6herkpwuh2p.us-east-1.rds.amazonaws.com/ah_data")
proxycurl_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@ah-database-prod.cluster-ro-c6herkpwuh2p.us-east-1.rds.amazonaws.com/enrichment_linkedin")
clonedflatdata_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@flat-data-prod-1-cluster.cluster-c6herkpwuh2p.us-east-1.rds.amazonaws.com/flatdata")
cloneddev_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@dev-graph-prod-1-cluster.cluster-c6herkpwuh2p.us-east-1.rds.amazonaws.com/dgraph")
crm_conn = PostGresConn(database_url="postgresql://read_user:read_myreo_dev@prod-crm-service.cluster-c6herkpwuh2p.us-east-1.rds.amazonaws.com:5432/crm_service")
clonedproduct_conn = PostGresConn(database_url = "postgresql://read_user:read_myreo_dev@product-activity-prod-1-cluster.cluster-c6herkpwuh2p.us-east-1.rds.amazonaws.com:5432/product_activity")
cloneddoc_conn = PostGresConn(database_url= "postgresql://read_user:read_myreo_dev@doc-activity-prod-1-cluster.cluster-c6herkpwuh2p.us-east-1.rds.amazonaws.com:5432/document_activity")
