# pythonprojs
pythonprojs
Python already has a built-in dictionary object that serves as a Hash Table. This project however implement a simple hashtable to illustrate the problem of designing nosql database
The main object uses hash table to store key,value pairs. But there are additional auxillary objects, for which I have leveraged in-built dictionary object for brevity.
The auxillary objects are:
1. dictionary object to store keys with ttl. Every get operations checks whether the key is valid to be returned or not
2. dictionary object where value of the main hashobject is key and values are the list of keys. It is similar to materialized views. Every put operation also appends this list. Every delete operations removes from this list
3. dictionary object which stores ttl as key a,d key as values. This is used in the background compaction process, which iterates through this list, and removes the expired objects from hashtable. This is not a full table scan, as it exits when a ttl which is higer than current time is encountered.
