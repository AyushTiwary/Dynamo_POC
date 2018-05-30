package com.knoldus

import java.util

import com.amazonaws.services.dynamodbv2.document.spec.{DeleteItemSpec, GetItemSpec}
import com.amazonaws.services.dynamodbv2.document.{Item, PrimaryKey, Table}
import com.amazonaws.services.dynamodbv2.model._
import com.knoldus.DynamoDBClient.{dynamoDB, table}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class Movie {

  def createTable(tableName: String): Future[Table] = Future {
    println("Attempting to create table, please wait....")
    dynamoDB.createTable(tableName,
      util.Arrays.asList(new KeySchemaElement("year", KeyType.HASH), // Partition key
        new KeySchemaElement("title", KeyType.RANGE)), // Sort key
      util.Arrays.asList(new AttributeDefinition("year", ScalarAttributeType.N),
        new AttributeDefinition("title", ScalarAttributeType.S)),
      new ProvisionedThroughput(10L, 10L))

  }.recoverWith {
    case ex => println("Unable to create the table")
      Future.failed(new Exception(ex.getMessage))
  }

  def addItem(year: Int, title: String, otherInfo: Map[String, Any]): Future[Item] = {
    val infoMap = new util.HashMap[String, Any]
    otherInfo.foreach {
      case (str, data) => infoMap.put(str, data)
    }
    Future {
      println("Adding a new item...")
      val outcome = table.putItem(new Item().withPrimaryKey("year", year, "title", title)
        .withMap("info", infoMap))
      println("PutItem succeeded: " + outcome.toString)
      outcome.getItem
    }
  }.recoverWith {
    case ex => Future.failed(new Exception(ex.getMessage))
  }

  def readItem(year: Int, title: String): Future[Item] = Future {
    val spec = new GetItemSpec().withPrimaryKey("year", year, "title", title)
    System.out.println("Attempting to read the item...")
    table.getItem(spec)
  }

  def deleteItem(year: Int, title: String) = Future {
    val deleteItemSpecForSuccess = new DeleteItemSpec().withPrimaryKey(new PrimaryKey("year", year, "title", title))
    println("Attempting a conditional delete...")
    table.deleteItem(deleteItemSpecForSuccess)
  }

  def deleteTable(tableName: String): Future[Unit] = Future{
    val table = dynamoDB.getTable(tableName)
      println("Attempting to delete table; please wait...")
      table.delete
      table.waitForDelete
  }

  def updateItem() = ??? //Todo(ayush)

  def getAll() = ??? /// Todo(ayush)

}
