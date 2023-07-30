
docker system prune -a

## kafka ayağa kaldırırken
1 - docker-compose -f docker_compose.yaml up
2 - docker ps çektikten sonra ayağa kalkmayan component varsa -> docker-compose restart <image-name> -> (image name ex. control-center )

### listen api 
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 app.py


### control center
http://localhost:9000