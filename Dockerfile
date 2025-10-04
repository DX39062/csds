# 按照要求，统一使用 ubuntu:20.04 作为基础镜像
FROM ubuntu:20.04

# 设置环境变量，防止 Python 输出被缓冲
ENV PYTHONUNBUFFERED 1

#加速下载
RUN sed -i 's/archive.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list 

# 更新包列表并安装系统依赖：python3 和 pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /code

# 为了利用 Docker 的层缓存机制，先只复制依赖文件
COPY ./app/requirements.txt /code/

# 安装 Python 依赖
RUN pip3 install --no-cache-dir -r requirements.txt

# 复制应用程序的源代码
COPY ./app /code/app

# 定义容器启动时要执行的命令
ENTRYPOINT ["python3", "-m", "app.cache_node"]