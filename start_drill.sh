INSTALL_DIR=./OUT

rm -rf $INSTALL_DIR
mkdir $INSTALL_DIR

tar xvzf distribution/target/*.tar.gz --strip=1 -C $INSTALL_DIR
$INSTALL_DIR/bin/sqlline -u jdbc:drill:zk=local -n admin -p admin
