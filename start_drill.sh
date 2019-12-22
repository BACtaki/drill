INSTALL_DIR=./OUT

## Kill existing drill processes
kill -9 $(pgrep -f drill)

## Remove installation dirs if exist
rm -rf $INSTALL_DIR
mkdir $INSTALL_DIR

## Unpack bins and install
tar xvzf distribution/target/*.tar.gz --strip=1 -C $INSTALL_DIR
$INSTALL_DIR/bin/sqlline -u jdbc:drill:zk=local -n admin -p admin
