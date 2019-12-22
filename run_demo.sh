## Stop and remove all docker containers
docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)

## Start ELK containers
pushd docker-elk
docker-compose up -d
popd

## Start drill
./start_drill.sh

## Clean up ELK containers
docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)
