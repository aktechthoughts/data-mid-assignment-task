import io
import pandas as pd
import psycopg2

from time import sleep
from boto3 import client

"""
This method is used to download files from s3 bucket.
"""

def download_files(bucket_name):
    conn = client('s3') 
    files = []
    for key in conn.list_objects(Bucket=bucket_name)['Contents']:
        if str(key['Key']).endswith('.tsv') :
            OBJECT_NAME=str(key['Key'])
            FILE_NAME=OBJECT_NAME.split('/')[1]
            print('Downloading : {}'.format(FILE_NAME))
            with open(str('/tmp/'+FILE_NAME), 'wb') as f:
                conn.download_fileobj(bucket_name, OBJECT_NAME, f)
            files.append(str('/tmp/'+FILE_NAME))

    return files


"""
This method is used create a table to load raw data downloaded from s3 bucket.
"""
def create_raw_data_table(cursor):
    cursor.execute("""
        DROP TABLE IF EXISTS raw_data;
        CREATE UNLOGGED TABLE raw_data (
            ts text,
            session_id text,
            event_name text,
            user_id text,
            article_id text,
            category text,
            title text,
            attributes text
        );
    """)



"""
This method loads data from raw_data table.
The json attributes are loaded in the separate column for further processing.
This table is dropped every time job runs.
"""
def load_stg_data_table(cursor):
    cursor.execute("""
		DROP TABLE IF EXISTS stg_data;
		create TEMPORARY table stg_data AS
		select 
			DATE(ts) as event_date,
			user_id,
			session_id,
			event_name,
			replace(cast(record->'id' as text),'"','') as article_id,
			replace(cast(record->'category' as text),'"','') as category,
			replace(cast(record->'title' as text),'"','') as title 
		from
		(
			SELECT  
				ts,
				user_id,
				session_id,
				event_name,
				cast(replace(TRIM(both '"' from "attributes"),'""','"') as json) as record 
			FROM public.raw_data
		) records;
    """)


"""
This method creates aggregate tables article_performance & user_performance if they
dont exists.
"""
def create_aggregate_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS  article_performance (
            article_id 	text,
            dt datE,
            title 	text,
            category varchar(50),
            card_views 	integer,
            article_views integer
        );
        CREATE TABLE IF NOT EXISTS user_performance(
            user_id text, 
            dt date, 	
            ctr decimal
        );"""
    )

"""
This method reads stg_data table and loads article_performace and user_performace.
"""

def load_aggregate_table(cursor):
    cursor.execute("""
        INSERT INTO user_performance 
        select
		card_viewed.user_id,
		card_viewed.event_date,
		sum(
			case when card_viewed.article_id = article_viewed.article_id and 
					  card_viewed.event_name in('my_news_card_viewed', 'top_news_card_viewed') and
					  article_viewed.event_name in('article_viewed') 
			     then 1.0
			else
				0.0
			end
		) / count(distinct card_viewed.article_id) as ctr
	from
		STG_DATA card_viewed left outer join
		STG_DATA article_viewed
	on
		card_viewed.user_id = article_viewed.user_id and
		card_viewed.event_date = article_viewed.event_date and
		card_viewed.article_id = article_viewed.article_id 
	where
		card_viewed.article_id is not null 
	group by    
		card_viewed.user_id,
		card_viewed.event_date;

	insert into article_performance
	select 
		article_id,
		event_date,
		title,
		category,
		sum(case when event_name in('my_news_card_viewed','top_news_card_viewed') then 1 else 0 end) as card_views,
		sum(case when event_name = 'article_viewed' then 1 else 0 end) as article_views
	from 
		STG_DATA
	group by 
		article_id,
		event_date,
		title,
		category;

    """)	

"""
This method reads the files downloaded from s3 and returns a pandas dataframe.
Column are renamed. Pandas df is required to work with method copy_from defined in next declaration.
"""
def file_to_pandas_df(file_name):
    df = pd.read_csv(file_name,sep='\t',names=['ts','session_id','event_name','user_id','attributes'],header=0)

    return df


# Adopted from https://gist.github.com/jamesdunham/adb6909837a40540e5be010f030a2e45
# Fastest method to copy data into postgresql

def copy_from(df: pd.DataFrame,
    table: str,
    connection: psycopg2.extensions.connection,
    chunk_size: int = 10000):

    cursor = connection.cursor()
    df = df.copy()
    escaped = {'\\': '\\\\', '\n': r'\n', '\r': r'\r', '\t': r'\t'}
    for col in df.columns:
        if df.dtypes[col] == 'object':
            for v, e in escaped.items():
                df[col] = df[col].str.replace(v, e)
    try:
        for i in range(0, df.shape[0], chunk_size):
            f = io.StringIO()
            chunk = df.iloc[i:(i + chunk_size)]
            # The default separator is a tab, and NULLs are indicated by the two character-string '\N'
            chunk.to_csv(f, index=False, header=False, sep='\t', na_rep='\\N', quoting=None)
            f.seek(0)
            cursor.copy_from(f, table, columns=list(df.columns))
            connection.commit()
    except psycopg2.Error:
        connection.rollback()
        cursor.close()




if __name__ == "__main__":

    sleep(10)
    try:
        # Just some sample code.
        with psycopg2.connect(user='user',
                            password='password',
                            host='postgres',
                            database='database') as connection:
            cursor = connection.cursor()
            # Print PostgreSQL Connection properties
            print(connection.get_dsn_parameters(), '\n')

            # Print PostgreSQL version
            cursor.execute('SELECT version();')
            record = cursor.fetchone()
            print('You are connected to - ', record, '\n')

            # Create tables for processing
            create_raw_data_table(cursor)
            create_aggregate_table(cursor)

            # Download file from S3 bucket
            files = download_files('upday-data-assignment')

            # Load data in raw_data table
            for fname in files:
                df = file_to_pandas_df(fname)
                copy_from(df,'raw_data',connection)

            # Load data in stg table and process
            load_stg_data_table(cursor)
            load_aggregate_table(cursor)

            cursor.execute('SELECT count(*) from stg_data;')
            record = cursor.fetchone()
            print('Loaded ', record, '\n')
            cursor.close()


    except (Exception, psycopg2.Error) as error:
        print('Error while connecting to PostgreSQL', error)

