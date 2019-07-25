require 'httparty'
require 'kafka_rest'
require 'json'

client = KafkaRest::Client.new('http://localhost:18082')

uri = 'https://opensky-network.org/api/states/all?lamin=49.82380908513249&lomin=-10.8544921875&lamax=59.478568831926395&lomax=2.021484375'

response = HTTParty.get(uri)
data = JSON.parse(response.body)



avro_schema = KafkaRest::AvroSchema.new({
                                            name: "FlightState",
                                            type: "record",
                                            fields: [
                                                {name: "icao24", type: "string"},
                                                {name: "longitude", type: "double"},
                                                {name: "latitude", type: "double"},
                                                {name: "callsign", type: "string"},
                                                {name: "on_ground", type: "boolean"},
                                            ]
                                        })

key_schema = KafkaRest::AvroSchema.new({
                                           name: "icao24",
                                           type: "record",
                                           fields: [
                                               {name: 'icao24', type: 'string'}
                                           ]
                                       })
states = data['states'].map do |state|
  KafkaRest::Message.new(key: {icao24: state[0]}, value: {icao24: state[0],
                                                          longitude: state[5],
                                                          latitude: state[6],
                                                          callsign: state[1],
                                                          on_ground: state[8]
  })
end

topic = client.topic("FlightState")
topic.produce_batch(states, value_schema: avro_schema, key_schema: key_schema)
