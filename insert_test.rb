require 'net/http'
require 'json'

rnd = Random.new

label_name = :insert_test
edges = []

limit = 200000
# from = rnd.rand(limit)
from = 1

edges = (1..limit).map do |n|
  ts, op, type = n, "insert", "edge"
  to = n
  props = { lastSeq: rnd.rand(100), success: true }

  [ts, op, type, from, to, label_name, props.to_json].join("\t")
end

def send(uri, payload, content_type)
  res = Net::HTTP.new(uri.host, uri.port).start do |client|
    request                 = Net::HTTP::Post.new(uri.path)
    request.body            = payload
    request["Content-Type"] = content_type
    client.request(request)
  end
  puts res.response.body
end

cmd = ARGV[0]
puts cmd
exit unless["insert", "delete"].include?(cmd)

if cmd == "insert"
  puts system("bash ./label.sh")
  sleep(3)

  uri = URI.parse("http://alpha-s2graph.daumkakao.io:9000/graphs/edges/bulk")
  ths = edges.each_slice(1000).map do |arr_of_edges|
    puts arr_of_edges.join("\n")
    send(uri, arr_of_edges.join("\n"), "text/plain")
  end
elsif cmd == "delete"
  deleteRequest = [{"timestamp": limit + 1, "label": "insert_test", "ids": [from]}]
  deleteUri = URI.parse("http://alpha-s2graph.daumkakao.io:9000/graphs/edges/deleteAll")

  puts deleteUri
  puts deleteRequest.to_json

  send(deleteUri, deleteRequest.to_json, "application/json")

  puts "Delete done"
end
