./elasticsearch-plugin  remove maxspeed-aggregation-plugin

 ./elasticsearch-plugin install "file:///opt/elasticsearch-maxspeed-aggregation-plugin-0.0.1-SNAPSHOT.zip"
 
  docker-compose restart elasticsearch
  
 docker-compose logs -f --tail 100 elasticsearch
 
 