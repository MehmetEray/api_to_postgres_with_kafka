import json

# JSON yapılandırma dosyasını açma ve yükleme
with open('config.json') as f:
    config = json.load(f)

# Örneğin veritabanı bilgilerini almak
db_host = config['database']['host']
db_port = config['database']['port']
db_username = config['database']['username']
db_password = config['database']['password']
db_name = config['database']['dbname']
db_target_table = config['database']['target_table']

# API bilgilerini almak
api_base_url = config['api']['base_url']
api_key = config['api']['api_key']

# Debug modunu almak
debug_mode = config['debug']

#kafka
topic = config['kafka']['topic']
bootstrap_servers = config['kafka']['bootstrap.servers']
group_id = "my_consumer_group"
auto_offset_reset = "earliest"
