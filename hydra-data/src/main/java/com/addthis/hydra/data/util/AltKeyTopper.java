/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.hydra.data.util;

import java.util.HashMap;
import java.util.Map;

import com.addthis.codec.Codec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AltKeyTopper {

    private static final Logger log = LoggerFactory.getLogger(AltKeyTopper.class);

    /**
     * Construct a top-K counter with a size of zero.
     */
    public AltKeyTopper() {
        this.lsentinel = new Node(null, Long.MIN_VALUE);
        this.rsentinel = new Node(null, Long.MAX_VALUE);
        this.locations = new HashMap<>();
        this.size = 0;
        lsentinel.next = rsentinel;
        rsentinel.prev = lsentinel;
    }

    /**
     * Is not used during the lifetime of the object.
     * Used for backwards compatibility when decoding an object.
     */
    @Codec.Set(codable = true)
    private HashMap<String, Long> map;

    /**
     * Is not used during the lifetime of the object.
     * Used for backwards compatibility when decoding an object.
     */
    @Codec.Set(codable = true)
    private long minVal;

    /**
     * Is not used during the lifetime of the object.
     * Used for backwards compatibility when decoding an object.
     */
    @Codec.Set(codable = true)
    private String minKey;

    /**
     * Is not used during the lifetime of the object.
     * Used for backwards compatibility when decoding an object.
     */
    @Codec.Set(codable = true)
    private boolean lossy;

    /**
     * These fields are used by the class. Any other
     * fields are for backwards compatibility.
     * keys are associated with values using the value sort order.
     * values are stored in reverse sorted order.
     * locations assign strings to positions in the values array.
     * size represents the number of items currently in the object.
     */
    private final Map<String, Node> locations;
    private final Node lsentinel;
    private final Node rsentinel;
    private int size;

    private final static class Node {
        long count;
        String key;
        Node prev;
        Node next;

        Node(String key, long count) {
            this.key = key;
            this.count = count;
        }

        /**
         * Remove a node from the linked list.
         */
        private void remove() {
            prev.next = next;
            next.prev = prev;
        }

        /**
         * Insert a node into the linked list.
         *
         * @param before predecessor of this node.
         */
        private void insert(Node before) {
            Node after = before.next;
            before.next = this;
            after.prev = this;
            this.prev = before;
            this.next = after;
        }

        /**
         * Determine if the current node must be moved.
         * If so then remove from list and insert at new location.
         */
        public void move() {
            if (count > next.count) {
                remove();
                findAndInsert(next);
            }
        }

        /**
         * Find correct location and insert into the list.
         *
         * @param hint where to begin searching.
         */
        public void findAndInsert(Node hint) {
            Node next = hint.next;
            while (next.count < count) {
                hint = next;
                next = hint.next;
            }
            insert(hint);
        }

    }

    /**
     * Updates the key {@code id} using a weight of 1.
     * If the key is already in the top-K then increment its weight.
     * Otherwise evict a key with the minimum value and replace
     * it with (key, minimum + 1).
     *
     * @param id
     * @return element dropped from top or null if accepted into top with no
     *         drops
     */
    public String increment(String id, int maxsize) {
        return increment(id, 1, maxsize);
    }

    /**
     * Updates the key {@code id} with the specified weight.
     * If the key is already in the top-K then update the weight.
     * Otherwise evict a key with the minimum value and replace
     * it with (key, minimum + weight).
     *
     * @param id
     * @param weight
     * @return element dropped from top or null if accepted into top with no
     *         drops
     */
    public String increment(String id, int weight, int maxsize) {
        if (maxsize < 1) {
            throw new IllegalArgumentException("maxsize must be a positive integer");
        }
        Node current = locations.get(id);
        if (current != null) {
            current.count += weight;
            current.move();
            return null;
        } else if (size < maxsize) {
            current = new Node(id, weight);
            locations.put(id, current);
            current.findAndInsert(lsentinel);
            size++;
            return null;
        } else {
            current = lsentinel.next;
            String prevId = current.key;
            locations.remove(prevId);
            locations.put(id, current);
            current.key = id;
            current.count += weight;
            current.move();
            return prevId;
        }
    }

    public int size() {
        return size;
    }

    /**
     * Returns either the estimated count associated with the key,
     * or null if the key is not present in the top-K items.
     *
     * @param key
     * @return
     */
    public Long get(String key) {
        Node current = locations.get(key);
        if (current != null) {
            return current.count;
        } else {
            return null;
        }
    }

}
