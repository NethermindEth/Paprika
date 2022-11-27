# Paprika

A low level Patricia tree, with no indirect db abstraction. 

This project aims in removing the abstraction of the underlying storage and make Patricia tree work directly with data. This means, that all node types: branches, extensions (yet to be implemented) and leafs use internal addressing, requiring no additional database to get the data or write them.

Additionally, this means that Paprika uses **almost no managed memory**, delegating everything to the underlying store, based on memory mapped files. There are no managed nodes, no cache and no encoding/decoding. Just key-value pairs stored in a structure directly mapped to disk. 

The database assumes no deletes and keeping the last root in the memory. If deletes or pruning is required, it can be added later on. For example, by copying only values that are alive. The roots can be captured and stored elsewhere. They map to a single long and one can easily amend paprika to get old data by its root.
