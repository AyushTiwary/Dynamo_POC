package com.knoldus

import java.io.File
import java.util

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.document.spec._
import com.amazonaws.services.dynamodbv2.document.utils._
import com.amazonaws.services.dynamodbv2.model._
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node.ObjectNode

import scala.util.{Failure, Success, Try}

object DynamoDBClient extends App {

  val client = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain())
  val dynamoDB = new DynamoDB(client)

  val attributeDefinitions = new util.ArrayList[AttributeDefinition]()
  attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"))

  val keySchema = new util.ArrayList[KeySchemaElement]
  keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH))

  val tableName = "Movies"

  val mov = new Movie
  //Creating a table
  mov.createTable(tableName)
  Thread.sleep(10000)

  //loading the table
  val table = dynamoDB.getTable(tableName)
  val parser = new JsonFactory().createParser(new File("src/main/resources/moviedata.json"))
  val rootNode: JsonNode = new ObjectMapper().readTree(parser)
  val iter = rootNode.iterator
  var currentNode: ObjectNode = _

  while (iter.hasNext) {
    currentNode = iter.next.asInstanceOf[ObjectNode]
    val year = currentNode.path("year").asInt
    val title = currentNode.path("title").asText
    val tryResponse = Try {
      table.putItem(new Item().withPrimaryKey("year", year, "title", title).withJSON("info", currentNode.path("info").toString))
    }

    tryResponse match {
      case Success(_) => println("PutItem succeeded: " + year + " " + title)
      case Failure(e) => println("Unable to add movie: " + year + " " + title)
        println(e.getMessage)
    }
  }
  parser.close()

  //uploading the item
  val year = 2015
  val title = "The Big New Movie"
  val infoMap = Map(("plot", "Nothing happens at all."), ("rating", 0))
  mov.addItem(year, title, infoMap)
  Thread.sleep(10000)

  //reading an item
  mov.readItem(year, title)
  Thread.sleep(10000)

  //updating an item
  val updateItemSpec = new UpdateItemSpec().withPrimaryKey("year", year, "title", title)
    .withUpdateExpression("set info.rating = :r, info.plot=:p, info.actors=:a")
    .withValueMap(new ValueMap().withNumber(":r", 5.5)
      .withString(":p", "Everything happens all at once.")
      .withList(":a", util.Arrays.asList("Larry", "Moe", "Curly")))
    .withReturnValues(ReturnValue.UPDATED_NEW)

  try {
    println("Updating the item...")
    val outcome = table.updateItem(updateItemSpec)
    println("UpdateItem succeeded:\n" + outcome.getItem.toJSONPretty)
  } catch {
    case e: Exception =>
      println("Unable to update item: " + year + " " + title)
      println(e.getMessage)
  }

  //deleting the item:: Conditional delete (we expect this to fail)
  val deleteItemSpecForFailure = new DeleteItemSpec().withPrimaryKey(new PrimaryKey("year", year, "title", title))
    .withConditionExpression("info.rating <= :val").withValueMap(new ValueMap().withNumber(":val", 5.0))

  try {
    println("Attempting a conditional delete...")
    table.deleteItem(deleteItemSpecForFailure)
    println("DeleteItem succeeded")
  } catch {
    case e: Exception =>
      println("Unable to delete item: " + year + " " + title)
      println(e.getMessage)
  }

  //deleting the item
  mov.deleteItem(year, title)
  Thread.sleep(10000)


  //deleting the table
  mov.deleteTable(tableName)
  Thread.sleep(20000)

}
