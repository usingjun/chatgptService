import json
import os

from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
import sys
import openai
import mysql.connector
from datetime import date

running = True

mydb = mysql.connector.connect(
    host=os.getenv('MYSQL_HOST'),
    port=os.getenv('MYSQL_PORT'),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_USER_PASSWORD'),
    database="sbb",
)

def init_consumer():
    conf = {'bootstrap.servers': "20.214.250.49:29092",
            'group.id': "chatgpt",
            'auto.offset.reset': 'smallest'}

    return Consumer(conf)


def request_to_chatGPT(data):
    title = data.get('title')
    content = data.get('content')
    response = openai.ChatCompletion.create(
        model="gpt-4", # gpt4는 현재 신청하여 승인된 사람에게만 권한이 있으므로 gpt4권한이 없다면 gpt-3.5-turbo로 설정
        messages=[
            {"role": "system", "content": "You are a consummate professional. Any question on the forum can be answered as a comment."},
            {"role": "user", "content": f"다음 질문은 게시판에 올라온 질문입니다. \n"
                                        f"---제목 : {title} \n"
                                        f"내용 : {content} \n"
                                        f"---\n"
                                        f" 해당 질문에 전문가가 되어 답변해주시기 바랍니다."},
        ]
    )

    return response["choices"][0]["message"]['content']


def save_db(question_id,response):
    mycursor = mydb.cursor()
    sql = "INSERT INTO answer (content, question_id, create_date) VALUES (%s, %s, %s)"
    val = (response, question_id, date.today())
    mycursor.execute(sql, val)

    mydb.commit()

def msg_process(msg):
    msg_data = json.loads(msg)

    command = msg_data.get('command')

    if command is None:
        print("there is no command")
        return

    if command == 'request_chatgpt':
        command_data = msg_data.get('data')
        if command_data is None:
            return
        response = request_to_chatGPT(command_data)
        question_id = command_data.get('id')
        if question_id is None:
            return
        save_db(question_id,response)
        print(response)
        return

    print(f"the command is not vaild :{command}")
    return

def consume_message(consumer,topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg.value())
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    consumer = init_consumer()
    consume_message(consumer,['chatgpt'])
# See PyCharm help at https://www.jetbrains.com/help/pycharm/