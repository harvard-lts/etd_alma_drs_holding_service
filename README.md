# ETD Alma Service
A Python service that assists in adding/updating drs hondings in Alma records for the ETD

<img src="https://github.com/harvard-lts/etd_alma_service/actions/workflows/pytest.yml/badge.svg">

<img src="https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/ives1227/23aeb140c43a9e3d808fef60a7b6556d/raw/covbadge.json">

### References

- Coverage badge adapted from [Ned Batchelder](https://nedbatchelder.com/blog/202209/making_a_coverage_badge.html)


### Run hello world example locally

- Clone this repo from github 
- Create the .env by copying the .example.env
`cp .env.example .env`
- Replace rabbit connect value with dev values (found in 1Password LTS-ETD)
- Replace the `CONSUME_QUEUE_NAME` and `PUBLISH_QUEUE_NAME` with a unique name for local testing (eg - add your initials to the end of the queue names)
- Start up docker  
`docker-compose -f docker-compose-local.yml up --build -d --force-recreate`

- Bring up [DEV ETD Rabbit UI](https://b-7ecc68cb-6f33-40d6-8c57-0fbc0b84fa8c.mq.us-east-1.amazonaws.com/)
- Look for `CONSUME_QUEUE_NAME` queue

- Exec into the docker container
`docker exec -it etd-alma-drs-holding-service bash`
- Run invoke task python script
`python3 scripts/invoke-task.py`

- Look for `PUBLISH_QUEUE_NAME` queue, and get the message in the RabbitMQ UI
- and/or tail <NEED LOG INFO> to see activity


### Manually placing a message on the queue

- Open the queue in the RabbitMQ UI
- Click on the `CONSUME_QUEUE_NAME` queue (the name that you assigned this env value to)
- Open Publish Message
- Set a property of `content_type` to `application/json`
- Set the Payload to the following JSON content
`{"id": "da28b429-e006-49a5-ae77-da41b925bd85","task": "etd-alma-drs-holding-service.tasks.add_holdings,"args": [{"hello":"world"}]}`

###  Unit Testing
- exec into docker
- `> pytest tests/unit`
- Note, integration tests are run as part of github actions, not locally
