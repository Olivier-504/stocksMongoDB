#import statements
import json
from bson import json_util
import bottle
from bottle import route, run, get, request, abort, post, response, put, delete
from pymongo import MongoClient

#Variables for CRUD operations that will be performed on database
connection = MongoClient('localhost', 27017)
db = connection['market']
collection = db['stocks']    


########### PART II #############

#method to insert a doc into collection(stocks) (will be paired with a 'create_doc' method)
def insert_doc(document):
  #if no exceptions, then:
  try:
    collection.save(document)
  #error handling below  
  except TypeError as te:
    abor(400, str(te))
  else:
    result = collection.find(document)
    return result
  
#method that calls 'insert_doc' and adds a document based off of a unique URI path and parses it as a Json obj
@route('/create', method='POST')
def create_doc():
  #if no exceptions, then:
  try:
    data = request.json
  #error handling below  
  except:
    abort(404, "Typo in data")
  #if no exceptions, then:  
  try:
    result = insert_doc(data)
    
    response.headers['Content-Type'] = 'application/json'
    return json.dumps(list(result), indent=4, default=json_util.default)
  except NameError as ne:
    abort(400, str(ne))
    
  
#method that reads and returns ticker symbols from medical labs & research industry in stocks collection
@route('/tik', method='GET')
def read_tik():
  #if no exceptions, then:
  try:
    #each document matching the above criteria will appear in the table
    for x in collection.find({"Industry": "Medical Laboratories & Research"}, {"Ticker": 1}):
      print(x)
  #error handling below  
  except NameError as ne:
    abort(404, str(ne))
    return ("No documents were found matching that industry name")
  
   

#method to update document in stocks collection
def change_doc(criteria, document):
  #if no exceptions, then:
  try:
    collection.update_one(criteria,{"$set" : document})
    result = collection.find(criteria)
  #error handling below
  except TypeError as te:
    abort(400, str(te))
  except Exception as we:
    abort(400, str(we))
  except:
    abort(400, "Typo in Request")
  else:
    return result
  
  
#method that calls 'change_doc' in order to update already create documents in the stocks collection
@route('/update', method='GET')
def update_doc():
  #if no exceptions, then:
  try:
    ticker = request.query.ticker
    volume = request.query.volume
    criteria = {"Ticker": ticker}
    change = {"Volume": volume}
    cursor = change_doc(criteria, change)
    
    if cursor:
      response.content_type = 'application/json'
      return json.dumps(list(cursor), indent=4, default=json_util.default)
    #error handling
    else:
      abort(404, "No documents found matching that ticker symbol.")
  except NameError as ne:
    abort(404, str(ne))
    

#method to remove a doc from the stocks collection. First searches and then returns.
def remove_doc(document):
  #if no exceptions, then:
  try:
    collection.delete_one(document)
    result = "True"
  #error handling below  
  except TypeError as te:
    abort(400, str(te))
    
  return result

#method that calls the remove_doc function and creates a URI specifically to remove a document
@route('/delete', method='GET')
def delete_doc():
  #if no exceptions, then:
  try:
    ticker = request.query.ticker
    result = remove_doc({"Ticker" : ticker})
    
    if result == "True":
      return "Successfully deleted\n"
    else:
      abort(404, "No document was found with that ticker symbol")
  #error handling below    
  except:
    abort(400, "Faulty Request")
    
    
########## PART III ###########
    
  
#method to find count of documents within specified range for "Simple Moving Average" field
@route('/read', method='GET')
def read_move_avg():

  result = collection.find({"50-Day Simple Moving Average": {"$gte": 0.005, "$lte": 0.006}}).count()
  print(result)
  
  
##########Method to print aggregate pipeline of total shares in healthcare industry    
@route('/aggShares', method='GET')
def aggShares():
  
    data = collection.aggregate([{"$match": {"Sector": "Healthcare"}},
                                 {"$project": {"_id": 0, "Industry": 1, "Shares Float": 1}},
                                 {"$group": {"_id": "$Industry", "Total Shares Float": {"$sum": "$Shares Float"}}}])
                   
    #each document matching the above criteria will appear in the table                            
    for x in data:
      print(x)
    


########### PART IV ############

  
#method that calls 'insert_doc' and adds a document based off of a unique URI path and parses it as a Json obj
@route('/stocks/api/v1.0/createStock/AA', method='POST')
def create_stock():
  #if no exceptions, then:
  try:
    data = request.json
  #error handling below  
  except:
    abort(404, "Typo in data")  #error handling
  #if no exceptions, then:
  try:
    result = insert_doc(data)
    
    response.headers['Content-Type'] = 'application/json'
    return json.dumps(list(result), indent=4, default=json_util.default)
  except NameError as ne:
    abort(400, str(ne))

#method to search stocks collection for a document
def find_stocks(document):
  
  result = []

  #if no exceptions, then:
  try:
    data = collection.find(document)
    for document in data:
      result.append(document)
  #error handling below    
  except TypeError as te:
    abort(400, str(te))
  else:
    return result
  
#method that calls 'find_stocks' in order to read and return a document from stocks collection
@route('/stocks/api/v1.0/getStock/AA', method='GET')
def read_stocks():
  #if no exceptions, then:
  try:
    ticker = request.query.ticker
    cursor = find_stocks({"Ticker": ticker})
    
    if cursor:
      response.content_type = 'application/json'
      return json.dumps(list(cursor), indent=4, default=json_util.default)
    #error handling below
    else:
      abort(404, "No documents were found matching that ticker symbol") #error handling
  except NameError as ne:
      abort(404, str(ne))

  
  
  
# 2nd method that calls 'change_doc' in order to update already create documents in the stocks collection
@route('/stocks/api/v1.0/updateStock/AA', method='GET')
def update_stock():
  #if no exceptions, then:
  try:
    name = request.query.name
    sector = request.query.sector
    criteria = {"Company": name}
    change = {"Sector": sector}
    cursor = change_doc(criteria, change)
    
    if cursor:
      response.content_type = 'application/json'
      return json.dumps(list(cursor), indent=4, default=json_util.default)
    #error handling below
    else:
      abort(404, "No documents found matching that ticker symbol.")
  except NameError as ne:
    abort(404, str(ne))
    
    
#method that calls the remove_doc function and creates a URI specifically to remove a document
@route('/stocks/api/v1.0/deleteStock/AA', method='GET')
def delete_stock():
  #if no exceptions, then:
  try:
    name = request.query.name
    result = remove_doc({"Company" : name})
    
    if result == "True":
      return "Successfully deleted\n"
    #error handling below
    else:
      abort(404, "No document was found with that business_id")
  except:
    abort(400, "Faulty Request")



######## This method reports summary information of stocks with the  below ticker symbols
@route('/stocks/api/v1.0/stockReport', method='GET')
def summary():
    records = collection.find({"Ticker": {"$in": ["AA", "BA", "T"]}},
                             {"_id": 0, "Industry": 1, "Ticker": 1, "Company": 1, "Price": 1})
    #each document matching the above criteria will appear in the table
    for x in records:
      print(x)


      
      
      
#######Method to retrieve top 5 stocks
@route('/stocks/api/v1.0/industryReport/telecom', method='GET')
def read_telecom():
  
    five = collection.aggregate([{"$match": {"Industry": "Telecom Services - Domestic"}},
                                {"$project": {"_id": 0, "Company": 1, "Return on Equity": 1}},
                               {"$sort": {"Return on Equity": -1}},
                               {"$limit": 5}])
    #each document matching the above criteria will appear in the table
    for x in five:
      print(x)

      
#######Method to retieve stocks in which AdventNet may be interested interested in
@route('/stocks/api/v1.0/portfolio/AdventNet', method='GET')
def advent_stocks():
  
  ten = collection.aggregate([{"$match": {"Industry": "Technical & System Software"}},
                              {"$project": {"_id": 0, "Industry": 1, "Company": 1, "Return on Equity": 1}},
                              {"$sort": {"Return on Equity": -1}},
                              {"$limit": 10}])
  #each document matching the above criteria will appear in the table
  for x in ten:
    print(x)

      
#Runs the script (main program)
if __name__ == '__main__':
  #app.run(debug=True)
  run(host='localhost', port=8080)  