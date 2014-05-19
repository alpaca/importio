import logging, json, importio, latch

# To use an API key for authentication, use the following code:
client = importio.importio(user_id="0be476b1-768d-45cd-8c34-88df0a7ceff6", api_key="rqC+3pL6qJRqk9mmpCOcnyMGhs62nC638ojezenB3CvvcYC78ktioXwlDYVi+1AMNJ/3LuJazy36+fmh5XXwJA==")

# Once we have started the client and authenticated, we need to connect it to the server:
client.connect()

# latch will prevent the script from exiting before all of our queries are returned
queryLatch = latch.latch(1)

# Define here a global variable that we can put all our results in to when they come back from
# the server, so we can use the data later on in the script
dataRows = []

# This method will receive each message that comes back from the queries
def callback(query, message):
  global dataRows
  
  # Disconnect messages happen if we disconnect the client library while a query is in progress
  if message["type"] == "DISCONNECT":
    print "Query in progress when library disconnected"
    print json.dumps(message, indent = 4)

  # Check the message we receive actually has some data in it
  if message["type"] == "MESSAGE":
    if "errorType" in message["data"]:
      # In this case, we received a message, but it was an error from the external service
      print "Got an error!" 
      print json.dumps(message["data"], indent = 4)
    else:
      # We got a message and it was not an error, so we can process the data
      print "Got data!"
      print json.dumps(message["data"], indent = 4)
      # Save the data we got in our dataRows variable for later
      dataRows.extend(message["data"]["results"])
  
  # When the query is finished, countdown the latch so the program can continue when everything is done
  if query.finished(): queryLatch.countdown()

# Issue queries to your data sources and with your inputs
# You can modify the inputs and connectorGuids so as to query your own sources

# Query for collegelist_test
# Query for tile US Private Schools
client.query({
  "connectorGuids": [
    "6fcbcfc9-dbea-4879-8004-ed6f5ba1f3e1"
  ],
  "input": {
    "school_name": "a"
  }
}, callback)

# Query for tile US Private Schools
client.query({
  "connectorGuids": [
    "6fcbcfc9-dbea-4879-8004-ed6f5ba1f3e1"
  ],
  "input": {
    "school_name": "b"
  }
}, callback)

# Query for tile US Private Schools
client.query({
  "connectorGuids": [
    "6fcbcfc9-dbea-4879-8004-ed6f5ba1f3e1"
  ],
  "input": {
    "school_name": "c"
  }
}, callback)

# Query for tile US Private Schools
client.query({
  "connectorGuids": [
    "6fcbcfc9-dbea-4879-8004-ed6f5ba1f3e1"
  ],
  "input": {
    "school_name": "d"
  }
}, callback)

# Query for tile US Private Schools
client.query({
  "connectorGuids": [
    "6fcbcfc9-dbea-4879-8004-ed6f5ba1f3e1"
  ],
  "input": {
    "school_name": "e"
  }
}, callback)

print "Queries dispatched, now waiting for results"

# Now we have issued all of the queries, we can "await" on the latch so that we know when it is all done
queryLatch.await()

print "Latch has completed, all results returned"

# It is best practice to disconnect when you are finished sending queries and getting data - it allows us to
# clean up resources on the client and the server
client.disconnect()

# Now we can print out the data we got
print "All data received:"
print json.dumps(dataRows, indent = 4)