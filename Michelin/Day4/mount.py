# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://<container>@<storage_account>.blob.core.windows.net",
  mount_point = "/mnt/<storage_account>/<container>",
  extra_configs = {"fs.azure.account.key.<storage_account>.blob.core.windows.net":""})
