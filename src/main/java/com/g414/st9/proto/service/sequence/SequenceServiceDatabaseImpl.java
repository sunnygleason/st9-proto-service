package com.g414.st9.proto.service.sequence;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
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

import com.g414.st9.proto.service.store.Key;

/**
 * CounterService takes on the role of key generator in st9, effectively making
 * it so that inserts are just 'insert', not 'select and update to get the id'
 * followed by 'insert'. This version is safe to run on multiple nodes, but will
 * result in sequences that do not necessarily match creation order. A future
 * version should probably use interleaved sequences to address this issue.
 */
public class SequenceServiceDatabaseImpl implements SequenceService {
    public static final long DEFAULT_INCREMENT = 100000L;
    protected final Map<String, Counter> counters = new ConcurrentHashMap<String, Counter>();
    protected final Map<String, Integer> typeCodes = new ConcurrentHashMap<String, Integer>();
    protected final Map<Integer, String> typeNames = new ConcurrentHashMap<Integer, String>();
    protected final SequenceHelper sequenceHelper;
    protected final IDBI database;
    protected final String prefix;
    protected final long increment;

    public SequenceServiceDatabaseImpl(SequenceHelper sequenceHelper,
            IDBI database, String prefix, long increment) {
        this.sequenceHelper = sequenceHelper;
        this.database = database;
        this.prefix = prefix;
        this.increment = increment;
    }

    public synchronized void bumpKey(final String type, long id)
            throws Exception {
        Counter counter = this.counters.get(type);
        if (counter == null) {
            this.nextKey(type);
        }

        this.counters.get(type).bumpKey(id);
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

    public Integer getTypeId(final String type, final boolean create)
            throws Exception {
        return getTypeId(type, create, true);
    }

    public Integer getTypeId(final String type, final boolean create,
            final boolean strict) throws Exception {
        if (type == null) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid entity 'type'")
                    .build());
        }

        if (typeCodes.containsKey(type)) {
            return typeCodes.get(type);
        }

        Integer result = database
                .inTransaction(new TransactionCallback<Integer>() {
                    @Override
                    public Integer inTransaction(Handle handle,
                            TransactionStatus status) throws Exception {
                        try {
                            return sequenceHelper.validateType(handle, prefix,
                                    typeCodes, typeNames, type, create);
                        } catch (WebApplicationException e) {
                            return null;
                        }
                    }
                });

        if (result == null && strict) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .entity("Invalid entity 'type': " + type).build());
        }

        return result;
    }

    public String getTypeName(final Integer id) throws Exception {
        if (typeNames.containsKey(id)) {
            return typeNames.get(id);
        }

        return database.inTransaction(new TransactionCallback<String>() {
            @Override
            public String inTransaction(Handle handle, TransactionStatus status)
                    throws Exception {
                try {
                    return SequenceHelper.getTypeName(handle, prefix,
                            typeNames, id);
                } catch (WebApplicationException e) {
                    return null;
                }
            }
        });
    }

    public void clear() {
        this.counters.clear();
        this.typeCodes.clear();
        this.typeNames.clear();
    }

    public Map<String, Counter> initializeCountersFromDb() {
        database.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status)
                    throws Exception {
                try {
                    List<Map<String, Object>> currentLimits = handle
                            .createQuery(prefix + "select_sequences").list();

                    List<Map<String, Object>> currentOffsets = handle
                            .createQuery(prefix + "select_max_key_ids").list();

                    Map<String, Counter> theCounters = computeCounters(
                            currentLimits, currentOffsets);

                    SequenceServiceDatabaseImpl.this.counters
                            .putAll(theCounters);

                    getCurrentCounters();

                    return null;
                } catch (WebApplicationException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        });
        return null;
    }

    public Map<String, Counter> getCurrentCounters() {
        return Collections.unmodifiableMap(counters);
    }

    private Counter createCounter(final String type) {
        final int typeId = this.database
                .withHandle(new HandleCallback<Integer>() {
                    @Override
                    public Integer withHandle(Handle handle) throws Exception {
                        return sequenceHelper.validateType(handle, prefix,
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

    private Map<String, Counter> computeCounters(
            List<Map<String, Object>> currentLimits,
            List<Map<String, Object>> currentOffsets) throws Exception {
        Map<Long, Long> lims = convert(currentLimits);
        Map<Long, Long> offs = convert(currentOffsets);

        Map<String, Counter> toReturn = new LinkedHashMap<String, Counter>();
        for (Map.Entry<Long, Long> entry : offs.entrySet()) {
            String typeName = this.getTypeName(entry.getKey().intValue());
            Long limit = lims.get(entry.getKey());
            Long base = limit - DEFAULT_INCREMENT;
            Long offset = offs.get(entry.getKey());

            if ("$schema".equals(typeName)) {
                base = Long.valueOf(0);
                offset += 1;
            }

            Counter theCount = new Counter(typeName, base, limit);
            theCount.bumpKey(offset);

            toReturn.put(typeName, theCount);
        }

        return toReturn;
    }

    private static Map<Long, Long> convert(List<Map<String, Object>> inputList) {
        Map<Long, Long> toReturn = new LinkedHashMap<Long, Long>();

        for (Map<String, Object> input : inputList) {
            Long type = null;
            Long value = null;

            for (Map.Entry<String, Object> entry : input.entrySet()) {
                if (entry.getKey().equals("_key_type")) {
                    type = Long.parseLong(entry.getValue().toString());
                } else {
                    value = Long.parseLong(entry.getValue().toString());
                }

            }

            toReturn.put(type, value);
        }

        return toReturn;
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

        public void bumpKey(long id) throws Exception {
            long wouldBe = base + offset.get();
            long diff = id - wouldBe;

            if (diff <= 0) {
                return;
            }

            if (id >= max) {
                throw new IllegalStateException("cannot move counter from "
                        + wouldBe + " to desired position " + id + " past "
                        + max);
            }

            this.offset.addAndGet(diff);
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

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();

            builder.append("Counter[type=");
            builder.append(type);
            builder.append(",base=");
            builder.append(base);
            builder.append(",offset=");
            builder.append(offset.get());
            builder.append(",max=");
            builder.append(max);
            builder.append("]");

            return builder.toString();
        }
    }
}
