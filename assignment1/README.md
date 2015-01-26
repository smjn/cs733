# Memcache clone

Assignment 1 of course Engineering a Cloud, CS733, IIT Bombay
Author:Sushant Mahajan
Discussed with:
* Harshit Pande			133050040
* Bharath Radhakrishnan	133050014

The project includes 2 files:
* server.go - This has the main code implementing the protocol mentioned below.
* server_test.go - Test cases of various commands and scenarios.

Memcache is synonymous with key value store that forms an integral part of any cloud infrastructure.

In this project we have implemented the following commands:

* set - This command allows user to insert a new key into the store and the reply from server is `OK <version>\r\n` where version is key version. Version is incremented on each set for each key individually.

* get - This command fetched the value corresponding to the key. If key is expired or not found, user is appropriately notified or the value is sent `VALUE <numbytes>\r\n<value bytes>\r\n`

* getm - This command fetches the value of the key along with other key parameters like time left to expiry, current version

* cas - Short for compare and swap. This takes the key name, version and new value as input and returns new version of the changed key. Errors and notified appropriately.

* delete - Deletes a key from the server, error if key is expired or not found.

All functions use locking mechanisms to provide concurrency. For performance reasons, if a user asks for a key and it is expired, the user is notified as not found and the key is deleted.

Note: Time taken to run all tesk cases 16.032 sec (150 concurrent clients sending 250 commands each)
