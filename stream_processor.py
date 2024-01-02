from kafka import KafkaConsumer, KafkaProducer
import requests, json

#configuration
bootstrap_servers = 'localhost:9092'
input_topic = 'forcast-weather-raw'
output_topic = 'forcast-weather-proc'

#consumer
consumer = KafkaConsumer(input_topic, bootstrap_servers=bootstrap_servers,value_deserializer=lambda x:json.loads(x.decode('utf-8')))

#producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda x: json.dumps(x).encode('utf-8'))


for message in consumer:
    #take json file
    json_data = message.value

    temperature_kelvin = float(json_data['main']['temp'])
    temperature_celsius = temperature_kelvin - 273.15
    print(f"Temperature in Kelvin: {temperature_kelvin} //// Temperature in Celsius: {temperature_celsius}")

    processed_data = {
        'city': json_data.get('name','Unknown City'),
        'temperature_celsius':round(temperature_celsius,2),
        'dt': json_data.get('dt','error no date')
    }

    producer.send(output_topic, value=processed_data)
    producer.flush()

consumer.close()
producer.close()