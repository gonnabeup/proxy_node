import os

LOCAL_DATABASE_URL = os.getenv('LOCAL_DATABASE_URL', 'sqlite:///data/proxy_node.db')
CACHE_ENABLED = os.getenv('CACHE_ENABLED', '1') == '1'
PROXY_HOST = os.getenv('PROXY_HOST', '0.0.0.0')
PROXY_API_HOST = os.getenv('PROXY_API_HOST', '0.0.0.0')
PROXY_API_PORT = int(os.getenv('PROXY_API_PORT', '8080'))
PROXY_API_TOKEN = os.getenv('PROXY_API_TOKEN', '')
PROXY_NODE_NAME = os.getenv('PROXY_NODE_NAME', 'local')
