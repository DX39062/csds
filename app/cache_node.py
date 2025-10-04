import os
import sys
import json
import logging
from hashlib import sha1
from threading import Lock

import requests
from flask import Flask, request, abort
from waitress import serve

# --- 环境变量和基础配置 ---
NODE_ID = os.environ.get("NODE_ID")
if not NODE_ID:
    sys.exit(1)

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format=f'%(asctime)s - %(levelname)s - [{NODE_ID}] - %(message)s',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# 从 NODE_ID 中提取主机名和端口
try:
    HOST, PORT_STR = NODE_ID.split(':')
    PORT = int(PORT_STR)
except (ValueError, IndexError):
    logger.error(f"无效的 NODE_ID 格式: {NODE_ID}")
    sys.exit(1)

ALL_NODES = [
    "cache-server-1:8000",
    "cache-server-2:8000",
    "cache-server-3:8000",
]

# --- 线程锁 和 requests Session ---
cache_lock = Lock() # 保护对 local_cache 的并发访问
# 创建一个 requests Session 对象，它会自动处理连接池和 keep-alive
http_session = requests.Session()

# --- 一致性哈希实现 ---
class ConsistentHash:
    def __init__(self, nodes=None, replicas=3):
        self.replicas = replicas
        self.ring = dict()
        if nodes:
            for node in nodes:
                self.add_node(node)
    def add_node(self, node):
        for i in range(self.replicas):
            key = self.gen_key(f"{node}:{i}")
            self.ring[key] = node
    def get_node(self, key_str):
        if not self.ring: return None
        key = self.gen_key(key_str)
        nodes = sorted(self.ring.keys())
        for node_key in nodes:
            if key <= node_key: return self.ring[node_key]
        return self.ring[nodes[0]]
    def gen_key(self, key_str):
        return sha1(key_str.encode()).hexdigest()[:10]

# --- Flask 应用和本地缓存 ---
app = Flask(__name__)
local_cache = {}
ch = ConsistentHash(ALL_NODES)

# --- 外部 API 端点定义 (供客户端使用) ---

@app.route('/', methods=['POST'])
def external_set():
    data = request.get_json()
    if not data or len(data) != 1:
        abort(400)
    key, value = list(data.items())[0]
    logger.info(f"收到外部 HTTP SET: key='{key}'")
    target_node = ch.get_node(key)

    if target_node == NODE_ID:
        # 数据属于本节点，直接调用内部处理函数
        return internal_set(key, value)
    else:
        # 数据不属于本节点，通过 HTTP 转发
        logger.info(f"转发 SET 至 [{target_node}]")
        try:
            url = f"http://{target_node}/internal/set/{key}"
            # 设置一个合理的超时时间
            response = http_session.post(url, json=value, timeout=2)
            response.raise_for_status() # 如果状态码不是 2xx，则抛出异常
            return response.content, response.status_code
        except requests.exceptions.RequestException as e:
            logger.error(f"转发 SET 至 [{target_node}] 失败: {e}")
            abort(500)

@app.route('/<key>', methods=['GET', 'DELETE'])
def external_get_delete(key):
    logger.info(f"收到外部 HTTP {request.method}: key='{key}'")
    target_node = ch.get_node(key)

    if target_node == NODE_ID:
        # 数据属于本节点，直接调用内部处理函数
        if request.method == 'GET':
            return internal_get(key)
        elif request.method == 'DELETE':
            return internal_delete(key)
    else:
        # 数据不属于本节点，通过 HTTP 转发
        logger.info(f"转发 {request.method} 至 [{target_node}]")
        try:
            url = f"http://{target_node}/internal/data/{key}"
            if request.method == 'GET':
                response = http_session.get(url, timeout=2)
            elif request.method == 'DELETE':
                response = http_session.delete(url, timeout=2)
            
            # 特殊处理 404 情况
            if response.status_code == 404:
                abort(404)
            
            response.raise_for_status()
            return response.content, response.status_code
        except requests.exceptions.RequestException as e:
            logger.error(f"转发 {request.method} 至 [{target_node}] 失败: {e}")
            abort(500)

# --- 内部 API 端点定义 (仅供节点间通信使用) ---

@app.route('/internal/set/<key>', methods=['POST'])
def internal_set_route(key):
    value = request.get_json()
    return internal_set(key, value)

@app.route('/internal/data/<key>', methods=['GET', 'DELETE'])
def internal_get_delete_route(key):
    if request.method == 'GET':
        return internal_get(key)
    elif request.method == 'DELETE':
        return internal_delete(key)

# --- 核心逻辑函数 (线程安全) ---

def internal_set(key, value):
    logger.info(f"[内部] SET: key='{key}'")
    with cache_lock:
        local_cache[key] = value
    return "OK", 200

def internal_get(key):
    logger.info(f"[内部] GET: key='{key}'")
    with cache_lock:
        if key in local_cache:
            return {key: local_cache[key]}, 200
        else:
            abort(404)

def internal_delete(key):
    logger.info(f"[内部] DELETE: key='{key}'")
    with cache_lock:
        result = "1" if local_cache.pop(key, None) is not None else "0"
    return result, 200

if __name__ == '__main__':
    logger.info(f"HTTP 服务器正在 0.0.0.0:{PORT} 上启动...")
    serve(app, host='0.0.0.0', port=PORT)

