package com.g414.st9.proto.service.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;

/**
 * CounterService takes on the role of key generator in st9, effectively making
 * it so that inserts are just 'insert', not 'select and update to get the id'
 * followed by 'insert'. This version is safe to run on multiple nodes, but will
 * result in sequences that do not necessarily match creation order. A future
 * version should probably use interleaved sequences to address this issue.
 */
public class CounterService {
    protected static final long DEFAULT_INCREMENT = 100000L;
    protected final Map<String, Counter> counters = new ConcurrentHashMap<String, Counter>();
    protected final Map<String, Integer> typeCodes = new ConcurrentHashMap<String, Integer>();
    protected final Map<Integer, String> typeNames = new ConcurrentHashMap<Integer, String>();
    protected final IDBI database;
    protected final String prefix;
    protected final long increment;

    public CounterService(IDBI database, String prefix) {
        this(database, prefix, DEFAULT_INCREMENT);
    }

    public CounterService(IDBI database, String prefix, long increment) {
        this.database = database;
        this.prefix = prefix;
        this.increment = increment;
    }

    public synchronized Key nextKey(final String type) throws Exception {
        if (type == null) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid entity 'type'")
                    .build());
        }

        Counter counter = counters.get(type);

        if (counter == null) {
            counter = createCounter(type);
            counters.put(type, counter);
        }

        Key nextKey = counter.getNext();

        if (nextKey == null) {
            counter = createCounter(type);
            counters.put(type, counter);

            nextKey = counter.getNext();
        }

        return nextKey;
    }

    public Key peekKey(final String type) throws Exception {
        Counter counter = counters.get(type);

        if (counter == null) {
            return null;
        }

        return counter.peekNext();
    }

    public Integer getTypeId(final String type) throws Exception {
        if (type == null) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid entity 'type'")
                    .build());
        }

        if (typeCodes.containsKey(type)) {
            return typeCodes.get(type);
        }

        return database.inTransaction(new TransactionCallback<Integer>() {
            @Override
            public Integer inTransaction(Handle handle, TransactionStatus status)
                    throws Exception {
                return SequenceHelper.validateType(handle, prefix, typeCodes,
                        typeNames, type, true);
            }
        });
    }

    public String getTypeName(final Integer id) throws Exception {
        if (typeNames.containsKey(id)) {
            return typeNames.get(id);
        }

        return database.inTransaction(new TransactionCallback<String>() {
            @Override
            public String inTransaction(Handle handle, TransactionStatus status)
                    throws Exception {
                return SequenceHelper
                        .getTypeName(handle, prefix, typeNames, id);
            }
        });
    }

    public void clear() {
        this.counters.clear();
        this.typeCodes.clear();
        this.typeNames.clear();
    }

    private Counter createCounter(final String type) {
        final int typeId = this.database
                .withHandle(new HandleCallback<Integer>() {
                    @Override
                    public Integer withHandle(Handle handle) throws Exception {
                        return SequenceHelper.validateType(handle, prefix,
                                typeCodes, typeNames, type, true);
                    }
                });

        long nextBase = this.database.withHandle(new HandleCallback<Long>() {
            @Override
            public Long withHandle(Handle handle) throws Exception {
                return SequenceHelper.getNextId(handle, prefix, typeId,
                        increment);
            }
        });

        return new Counter(type, nextBase, nextBase + increment);
    }

    protected class Counter {
        private final String type;
        private final long base;
        private final long max;
        private final AtomicLong offset = new AtomicLong();

        public Counter(String type, long base, long max) {
            this.type = type;
            this.base = base;
            this.max = max;
        }

        public Key getNext() throws Exception {
            long next = base + offset.incrementAndGet();

            if (next < max) {
                return new Key(type, next);
            }

            return null;
        }

        public Key peekNext() throws Exception {
            long next = base + offset.get() + 1L;

            if (next < max) {
                return new Key(type, next);
            }

            return null;
        }
    }
}
