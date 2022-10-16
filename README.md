### Pre-requisites :
  - python >= 3.8.*
    - virutalenv or virtualenvwrapper
  - docker == 20.10.18
    - docker compose (plugin)

### Execute step by step on your linux machine:
- ```cd $HOME  && git clone https://github.com/mnemonico/blablacar_usecase1.git && cd blablacar_usecase1 && export WORKSPACE=\`pwd\` ```
- ```virtualenv venv``` or ```mkvirtualenv venv && workon venv``` if you've virtualenvwrapper
- ```pip install -r requirements.txt``` 
- ```docker compose -f docker-compose.yml up -d```
- ```docker ps``` (make sure containers are up)
- to execute the api call and extarction with database load ```python extract_code/dag_executor.py```
- after the end of the previous execution, pass to the browser then enter the url
  - http://0.0.0.0:8080/
    - username: user@domain.com
    - password: SuperSecret
  - once connected to pgadmin; connect to the local database instance
    - username: postgres
    - password: postgres
    - database: postgres
- check table ```public.ods_line``` on ```postgres``` database
- to test the simple **dag** with (3 tasks) without any setup (usage via webserver and scheduler up)
  - cd $WORKSPACE && python extract_code/dag_executor.py 




[usecase github repository](https://github.com/mnemonico/blablacar_usecase1.git)