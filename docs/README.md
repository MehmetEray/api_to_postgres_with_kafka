## delete all images
docker system prune -a

## run
### 1 - docker-compose -f docker_compose.yaml up
### 2 - docker ps çektikten sonra ayağa kalkmayan component varsa -> docker-compose restart <image-name> -> (image name ex. control-center )

### producer
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 api_to_producer.py

### consumer
python3 consumer_to_postgres.py

### kafdrop
http://localhost:9000
