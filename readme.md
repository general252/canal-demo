##### cancel 模拟MySQL从服务器拉取同步数据示例

1. start database

mysqlcnf/mysql.cnf

```
[mysqld]
server-id=1
gtid_mode=ON
enforce-gtid-consistency=ON

skip-host-cache
skip-name-resolve
```

```
docker run -itd \
           -p 3306:3306 \
           -v $PWD/mysqlcnf:/etc/mysql/conf.d \
           -v $PWD/mysqldata:/var/lib/mysql \
           -e MYSQL_ROOT_PASSWORD="123456" \
           mysql:8.0.20
```

2. create user

```
CREATE USER 'slave_user'@'%' IDENTIFIED WITH caching_sha2_password BY 'slave_password';
GRANT REPLICATION SLAVE ON *.* TO 'slave_user'@'%';
GRANT SELECT ON *.* TO 'slave_user'@'%';
flush privileges;
```

3. 输出示例
```
2022/12/15 18:47:01 event_handler.go:130: OnGTID, set: 3f468344-675e-11eb-b210-80ce62f266bc:319943
2022/12/15 18:47:01 event_handler.go:75: OnRow(test_auth_aaa.stu) insert test_auth_aaa.stu [[1 tina]]
2022/12/15 18:47:01 event_handler.go:57: ==执行的SQL== sql: [INSERT INTO stu (id,name) Values (?,?)] args: [[1,"tina"]]
2022/12/15 18:47:01 event_handler.go:83: 添加 test_auth_aaa.stu {
  "id": 1,
  "name": "tina"
}
2022/12/15 18:47:01 event_handler.go:125: OnXID, pos: (binlog.000249, 76908443)
2022/12/15 18:47:01 event_handler.go:136: OnPosSynced, pos: (binlog.000249, 76908443), set: 3f468344-675e-11eb-b210-80ce62f266bc:1-319943, force: false       

...
2022/12/15 18:47:25 event_handler.go:130: OnGTID, set: 3f468344-675e-11eb-b210-80ce62f266bc:319949
2022/12/15 18:47:25 event_handler.go:57: ==执行的SQL== sql: [UPDATE stu SET id=?,name=? WHERE id=?] args: [[1,"tony",1]]
2022/12/15 18:47:25 event_handler.go:96: [test_auth_aaa.stu] 修改前: {
  "id": 1,
  "name": "tina"
}
修改后: {
  "id": 1,
  "name": "tony"
}
2022/12/15 18:47:25 event_handler.go:125: OnXID, pos: (binlog.000249, 76915446)
2022/12/15 18:47:25 event_handler.go:136: OnPosSynced, pos: (binlog.000249, 76915446), set: 3f468344-675e-11eb-b210-80ce62f266bc:1-319949, force: false       

...

2022/12/15 18:49:21 event_handler.go:130: OnGTID, set: 3f468344-675e-11eb-b210-80ce62f266bc:319973
2022/12/15 18:49:21 event_handler.go:75: OnRow(test_auth_aaa.stu) delete test_auth_aaa.stu [[1 tony]]
2022/12/15 18:49:21 event_handler.go:57: ==执行的SQL== sql: [DELETE FROM stu WHERE id=?] args: [[1]]
2022/12/15 18:49:21 event_handler.go:89: 删除 test_auth_aaa.stu {
  "id": 1,
  "name": "tony"
}
2022/12/15 18:49:21 event_handler.go:125: OnXID, pos: (binlog.000249, 76946785)
2022/12/15 18:49:21 event_handler.go:136: OnPosSynced, pos: (binlog.000249, 76946785), set: 3f468344-675e-11eb-b210-80ce62f266bc:1-319973, force: false       

```