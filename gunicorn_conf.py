bind = "127.0.0.1:8000"
workers = 1
worker_class = "uvicorn.workers.UvicornWorker"
accesslog = "/var/www/html/enterprise-messenger/logs/access.log"
errorlog = "/var/www/html/enterprise-messenger/logs/error.log"
timeout = 120
keepalive = 5