::start redis\redis-server

cd redis
redis-server --service-install redis.windows.conf --loglevel verbose
redis-server --service-start