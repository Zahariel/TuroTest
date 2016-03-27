Simple Database Program

Execution:

net.zag.paul.TuroTest is the executable. It's self-contained, so you should be able to do

> javac -sourcepath src -d bin src\net\zag\paul\TuroTest.java
> cd bin
> java net.zag.paul.TuroTest

to run it. Or of course you can import it as an Eclipse project. Note that Java 8 is required either way.

Design Notes:

This represents the database as a series of Transactions. SET and UNSET only change the first (most recent) Transaction: SET adds the value, and UNSET actually sets the value to null, which is used as a tombstone. (It is safe to use null as a tombstone because the input format provides no way to input null as an actual value.) GET and NUMEQUALTO look through each Transaction in turn, looking for an appropriate binding. The counts keeps track of how many bindings use each value, so that NUMEQUALTO can run quickly. Whenever a value is SET or UNSET, the counts of the most recent transaction are updated to decrement the old value (if any) and increment the new value (if any).

The last Transaction is actually the real database. It uses a special Map that detects attempts to insert a tombstone value (null for values, 0 for counts) and treats it as removing that key instead. Its constructor takes a Predicate that determines whether a value is a tombstone.

BEGIN, ROLLBACK, and COMMIT manipulate the list of Transactions. BEGIN adds a new empty transaction onto the front, while ROLLBACK discards the first Transaction without further effect. Since SET and UNSET only affect the first Transaction, this has the result of scoping Transactions appropriately. COMMIT merges each Transaction in turn into the next one, until the final Transaction (being the sum of all the active Transactions) is collapsed into the real database. At this point the special processing for tombstones in the real database takes effect, so that any keys that were UNSET by the Transaction stack are actually removed, and any values whose count dropped to 0 are also removed from counts.

Considerations for Extension:

The COMMIT command was defined to close all extant transactions, rather than just the most recent. If we wanted to do only the most recent, the change would be just to collapse the first two transactions in Database.commitTransaction(), rather than using stream().reduce() to collapse the entire list.

This implementation is (clearly) not threadsafe. The given definition of transactions as per-database, not per-user, makes multithreading a strange concept in the first place, but if we wanted to support it, we could change Transaction to use ConcurrentMaps instead of ordinary Maps. (This also requires a more explicit implementation of tombstones, perhaps using Optional, because ConcurrentMap does not support null values.) Some synchronization would also be required around the list of Transactions itself; the only sane way to do this would also cause COMMIT to be an atomic operation, which is probably what you wanted anyway.

Some thought could be given to making the list of Transactions a tree-structure instead, so that multiple simultaneous users could have separate Transaction sessions. (In this environment, each user would have their own transaction list, some suffix of which may be shared with other users.) This raises the question of what to do when one user changes a value from underneath another user's Transaction. The counts in a Transaction would probably be more easily implemented as deltas rather than the current implementation of new values. 

