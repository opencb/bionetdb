package org.opencb.bionetdb.core.api;

import org.opencb.bionetdb.core.models.Node;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public abstract class NetworkDBIterator implements Iterator<Node>, AutoCloseable {
    public static final EmptyNetworkDBIterator EMPTY_ITERATOR = new EmptyNetworkDBIterator();
    private List<AutoCloseable> closeables = new ArrayList<>();

    @Override
    public void close() throws Exception {
        for (AutoCloseable closeable: closeables) {
            closeable.close();
        }
    }

    public static NetworkDBIterator emptyIterator() {
        return EMPTY_ITERATOR;
    }

    private static class EmptyNetworkDBIterator extends NetworkDBIterator {
        EmptyNetworkDBIterator() {
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Node next() {
            throw new NoSuchElementException("Empty iterator");
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
