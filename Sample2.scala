// Databricks notebook source
//To create myScope1 -> azureDBURL/#secrets/CreateScope

import dbutils._

dbutils.secrets.get("myScope1", "f1sgdrd-sqldb-connection")



