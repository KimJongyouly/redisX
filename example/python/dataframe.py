
#  redisx-cli -x DF.FROMPARQUET df_test < ~/Downloads/kapt.parquet

from redis_df import redis_df

my_df = redis_df('df_test')

df = my_df.query('SELECT count(*) as cnt FROM self limit 100 ', profile=True)
print(df)

df = my_df.query('SELECT kapt_code, kapt_name, kapt_addr FROM self limit 100 ', profile=True)
print(df)

df = my_df.query('SELECT * FROM self limit 100 ', profile=True)
print(df)
