tar xvzf distribution/target/*.tar.gz --strip=1 -C /opt/drill
/opt/drill/bin/sqlline -u jdbc:drill:zk=local -n admin -p admin
