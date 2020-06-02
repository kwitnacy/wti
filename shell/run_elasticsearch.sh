sudo docker run --network host --name elastic --rm -e "http.port=10000" -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.6.2
