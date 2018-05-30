package com.knoldus

import java.io.File
import java.util

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item}
import com.amazonaws.services.dynamodbv2.model._
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
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

  //Creating a table

  val tryTableDescription = Try {
    println("Attempting to create table, please wait....")
    val table = dynamoDB.createTable(tableName,
      util.Arrays.asList(new KeySchemaElement("year", KeyType.HASH), // Partition key
        new KeySchemaElement("title", KeyType.RANGE)), // Sort key
      util.Arrays.asList(new AttributeDefinition("year", ScalarAttributeType.N),
        new AttributeDefinition("title", ScalarAttributeType.S)),
      new ProvisionedThroughput(10L, 10L))
    table.waitForActive()
  }

  tryTableDescription match {
    case Success(tableDescription) => println("Success. Table status:  " + tableDescription)
    case Failure(ex) => println("Exception in creating the table : " + ex.getMessage)
  }

  //updating the table
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
}
