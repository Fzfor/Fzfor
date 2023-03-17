启动maxwell
bin/maxwell --config config.properties --daemon
/opt/module/maxwell/bin/maxwell --config /opt/module/maxwell/config.properties --daemon

Maxwell 提供了 bootstrap 功能来进行历史数据的全量同步 命令如下:
bin/maxwell-bootstrap --config config.properties --database gmall --table user_info