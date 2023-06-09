from celery import Celery
import os

app = Celery()
app.config_from_object('celeryconfig')


@app.task(serializer='json',
          name='etd-alma-drs-holding-service.tasks.add_holdings')
def invoke_dims(message):
    print("message")
    print(message)
    invoke_hello_world()


# To be removed when real logic takes its place
def invoke_hello_world():

    # For 'hello world', we are also going to place a
    # message onto the etd_ingested_into_drs queue
    # to allow the pipeline to continue
    new_message = {"hello": "from etd-alma-drs-holding-service"}
    app.send_task("tasks.tasks.do_task", args=[new_message], kwargs={},
                  queue=os.getenv("PUBLISH_QUEUE_NAME"))
