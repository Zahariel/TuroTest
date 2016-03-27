/**
 * 
 */
package net.zag.paul;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.function.Predicate;

/**
 * @author Paul
 */
public class TuroTest
{
    
    /**
     * This is a version of HashMap which works the same as the normal one
     * except that an attempt to set a "fake" value is treated as removing
     * the key instead.
     * 
     * This really should be done with com.google.common.collect.ForwardingMap
     * but I didn't want to introduce the dependency.
     *
     * @param <K> The type of keys
     * @param <V> The type of values
     */
    private static class HashMapRemoveFakes<K,V> extends HashMap<K,V>
    {

        private static final long serialVersionUID = 1L;
        private Predicate<? super V> isFake;
        
        public HashMapRemoveFakes(Predicate<? super V> isFake)
        {
            super();
            this.isFake = isFake;
        }
        
        @Override
        public V put (K key, V value)
        {
            if (isFake.test(value))
            {
                return this.remove(key);
            }
            else 
            {
                return super.put(key, value);
            }
        }
        
        /**
         * Naive implementation of putAll that actually calls the crocked
         * version of put().
         * @param other
         */
        @Override
        public void putAll (Map<? extends K, ? extends V> other)
        {
            for (Map.Entry<? extends K, ? extends V> entry : other.entrySet())
            {
                put (entry.getKey(), entry.getValue());
            }
        }
    }
    
    public static class Database
    {
        
        /**
         * A Transaction is the combination of some data values and the changes
         * to counts caused by those data values.
         */
        private static class Transaction
        {
            public Map<String, String> data;
            public Map<String, Integer> counts;
            
            public Transaction(boolean isBase)
            {
                if (isBase)
                {
                    data = new HashMapRemoveFakes<>(Objects::isNull);
                    counts = new HashMapRemoveFakes<>(i -> i == null || i == 0);
                }
                else
                {
                    data = new HashMap<>();
                    counts = new HashMap<>();
                }
            }
        }
        
        
        // Transaction are stored from newest to oldest
        // The last Map is the "real" database
        private List<Transaction> transactions;
        
        public Database()
        {
            transactions = new LinkedList<>();
            
            // We use the crocked version of HashMap for the main database
            // so that unset names are actually removed and non-existent values
            // are removed from counts
            transactions.add(new Transaction(true));
        }
        
        public void set(String name, String value)
        {
            Optional<String> old = getInternal(name);
            transactions.get(0).data.put(name, value);
            if (old.isPresent())
            {
                decrementCount(old.get());
            }
            incrementCount(value);
        }
        
        private void incrementCount(String value)
        {
            int count = numEqualTo(value);
            transactions.get(0).counts.put(value, count + 1);
        }

        private void decrementCount(String value)
        {
            int count = numEqualTo(value);
            // if this drops to 0, the 0 is treated as a tombstone by the
            // real database
            transactions.get(0).counts.put(value, count - 1);
        }

        public String get(String name)
        {
            return getInternal(name).orElse("NULL");
        }

        public Optional<String> getInternal(String name)
        {
            for(Transaction trans : transactions)
            {
                if (trans.data.containsKey(name))
                {
                    return Optional.ofNullable(trans.data.get(name));
                }
            }
            return Optional.empty();
        }
        
        public void unset(String name)
        {
            Optional<String> old = getInternal(name);
            // null is used here as a tombstone
            transactions.get(0).data.put(name, null);
            if (old.isPresent())
            {
                decrementCount(old.get());
            }
        }
        
        public int numEqualTo(String value)
        {
            return numEqualToInternal(value).orElse(0);
        }

        public Optional<Integer> numEqualToInternal(String value)
        {
            for (Transaction trans : transactions)
            {
                if (trans.counts.containsKey(value))
                {
                    return Optional.ofNullable(trans.counts.get(value));
                }
            }
            return Optional.empty();
        }
        
        public void beginTransaction()
        {
            // This uses the normal HashMap so that a transaction can represent
            // the removal of elements
            transactions.add(0, new Transaction(false));
        }
        
        public boolean rollbackTransaction()
        {
            if (transactions.size() == 1)
            {
                return false;
            }
            transactions.remove(0);
            return true;
        }

        public boolean commitTransaction()
        {
            if (transactions.size() == 1)
            {
                return false;
            }
            
            // This merges each transaction into the next one, finally merging
            // the sum of all transactions into the base database
            Transaction result = transactions.stream().reduce((trans, base) -> {
                base.data.putAll(trans.data);
                base.counts.putAll(trans.counts);
                return base;
            }).get();
            
            transactions.clear();
            transactions.add(result);
            return true;
        }
    }
    
    /**
     * Main interactive loop of the program
     * 
     * @param args
     */
    public static void main(String[] args)
    {
        
        // make DB
        Database db = new Database();

        // run interactive loop
        try (Scanner input = new Scanner(System.in))
        {
            while (input.hasNext())
            {
                String command = input.next();
                switch (command.toUpperCase())
                {
                    case "END":
                    {
                        System.exit(0);
                        break;
                    }
                    case "SET":
                    {
                        String name = input.next();
                        String value = input.next();
                        db.set(name, value);
                        break;
                    }
                    case "GET":
                    {
                        String name = input.next();
                        System.out.println(db.get(name));
                        break;
                    }
                    case "UNSET":
                    {
                        String name = input.next();
                        db.unset(name);
                        break;
                    }
                    case "NUMEQUALTO":
                    {
                        String value = input.next();
                        System.out.println(db.numEqualTo(value));
                        break;
                    }
                    case "BEGIN":
                    {
                        db.beginTransaction();
                        break;
                    }
                    case "ROLLBACK":
                    {
                        if (!db.rollbackTransaction())
                        {
                            System.out.println("NO TRANSACTION");
                        }
                        break;
                    }
                    case "COMMIT":
                    {
                        if (!db.commitTransaction())
                        {
                            System.out.println("NO TRANSACTION");
                        }
                        break;
                    }
                    default:
                    {
                        System.out.println("Unexpected command: " + command);
                    }
                }
            }
        }
    }
}
