docker build -t docker_wine .

docker save docker_wine > wine_train.tar

docker run --name docker_wine docker_wine
