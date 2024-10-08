# CPD Projects

CPD Projects of group T04G06;.

The second project, carried out within the scope of the Parallel and Distributed Computing course, aims to develop a persistent distributed key-value store for a large cluster. Key-value simply implies a storage where arbitrary data objects are stored and associated with a key, which subsequently allows their access. In order to guarantee persistence, key-value pairs must be stored in persistent storage, such as an HDD or SSD. By distributed, we mean that the storage is partitioned between different nodes of the cluster. The service is ready to handle simultaneous requests and tolerate various failures, such as node failures and message loss.
To be able to test key-value storage, it was necessary to develop a TestClient. Essentially, what this should allow you to do is invoke any of the membership events (join or leave), as well as invoke any of the operations on key-value pairs (put (saves file in the system), get (reads system file ) and delete (deletes file from the system)).
The Java programming language was used.

- Tomás Gonçalves	 
- Alexandra Ferreira 	
- Vasco Teixeira	
