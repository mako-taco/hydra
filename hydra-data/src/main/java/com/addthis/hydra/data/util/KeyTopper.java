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

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.addthis.codec.Codec;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;

public final class KeyTopper implements Codec.Codable {

    /**
     * Construct a top-K counter with a size of zero.
     * The key topper will allocate memory upon the first insert operation.
     */
    public KeyTopper() {
        values = new long[0];
        locations = new ObjectIntOpenHashMap<>(0);
        keys = new String[0];
        size = 0;
        random = new Random();
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
    private String[] keys;
    private long[] values;
    private ObjectIntOpenHashMap<String> locations;
    private int size;
    private Random random;
    
    @Override
    public String toString() {
        return "topper(keys: " +
               Arrays.toString(keys) +
               " values: " +
               Arrays.toString(values) + ")";
    }

    /**
     * returns the list sorted by greatest to least count.
     */
    public Map.Entry<String, Long>[] getSortedEntries() {
        Map.Entry<String, Long> e[] = new Map.Entry[size];
        for(int i = 0; i < size; i++) {
            e[i] = new AbstractMap.SimpleImmutableEntry<>(keys[size - i - 1], values[size - i - 1]);
        }
        return e;
    }

    /**
     * Resize the collections when necessary.
     * If there is remaining capacity in the data structure then do not resize.
     * If the new size is less than or equal to the current capacity then do not resize.
     * Otherwise adjust the capacity to the new size.
     *
     * @param newsize
     */
    private void resize(int newsize) {
        if (size < values.length || newsize <= values.length) {
            return;
        }
        long[] newValues = new long[newsize];
        ObjectIntOpenHashMap<String> newLocations = new ObjectIntOpenHashMap<>((int) (newsize / 0.75), 1.0f);
        String[] newNames = new String[newsize];
        System.arraycopy(values, 0, newValues, 0, size);
        System.arraycopy(keys, 0, newNames, 0, size);
        newLocations.putAll(locations);
        values = newValues;
        keys = newNames;
        locations = newLocations;
    }

    /**
     * The value at one position may no longer be in the correct sorted order.
     * Test that value and move elements around to restore the sorted order.
     *
     * @param target index of the value that may be incorrect
     */
    private void reindex(int target) {
        String key = keys[target];
        long value = values[target];
        int position = target;
        while (position > 0 && value > values[position - 1]) {
            position--;
        }
        if (position != target) {
            System.arraycopy(keys, position, keys, position + 1, target - position);
            System.arraycopy(values, position, values, position + 1, target - position);
            for(int i = position + 1; i <= target; i++) {
                locations.put(keys[i], i);
            }
            keys[position] = key;
            values[position] = value;
            locations.put(key, position);
        }
    }

    /**
     * Returns the index of the minimum value for eviction.
     * Select randomly if there are multiple minimum values.
     */
    private int selectMinElement() {
        int position = size - 1;
        int retval;
        long value = values[position];
        while (position > 0 && value == values[position - 1]) {
            position--;
        }
        if (position == size - 1) {
            retval = position;
        } else {
            retval = position + random.nextInt(size - 1 - position);
        }
        assert(retval >= 0);
        assert(retval < values.length);
        return retval;
    }

    /**
     * Inserts the key {@code key} with a corresponding weight.
     * This will evict another key with the current minimum weight.
     * If {@code additive} is true then the new value associated
     * with {@code key} is the current minimum + weight. Otherwise
     * the new value is the weight. The correct implementation of
     * the top-K data structure assumes that {@code additive} is true.
     *
     * @param id        new key to insert
     * @param weight    weight associated with key
     * @param additive  see comment above
     * @return          old key that is removed
     */
    private String replace(String id, long weight, boolean additive) {
        // select the element for eviction
        int position = selectMinElement();
        // ensure that the element at position (size - 1) will be evicted
        String evicted = keys[position];
        keys[position] = keys[size - 1];
        locations.put(keys[position], position);
        locations.remove(evicted);
        // insert the new element
        values[size - 1] = weight + (additive ? values[size - 1] : 0);
        keys[size - 1] = id;
        locations.put(id, size - 1);
        reindex(size - 1);
        return evicted;
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
        int position = locations.getOrDefault(id, -1);
        if (position >= 0) {
            values[position] += weight;
            return null;
        }
        resize(maxsize);
        if (size < values.length) {
            values[size] = weight;
            keys[size] = id;
            locations.put(id, size);
            size++;
            return null;
        }
        return replace(id, weight, true);
    }

    /**
     * Increments the count for 'ID' in the top map if 'ID' already exists in
     * the map.
     *
     * @param id the id to increment if it already exists in the map
     * @return whether the element was in the map
     */
    public boolean incrementExisting(String id) {
        int position = locations.getOrDefault(id, -1);
        if (position >= 0) {
            values[position]++;
            reindex(position);
            return true;
        } else {
            return false;
        }
    }

    /**
     *
     * This method is maintained for legacy purposes although it does not
     * conform to the standard behavior of a top-K data structure.
     * It adds {@code id} the data structure if (1) there are more empty slots
     * or (2) count > smallest top count in the list. A correct top-K update
     * operation should always insert the new key into the data structure.
     * Use {@link #increment(String, int)} or {@link #increment(String, int, int)}
     * to maintain the correct semantics for top-K items.
     *
     * @param id
     * @param count
     * @return element dropped from top or null if accepted into top with no
     *         drops. returns the offered key if it was rejected for update
     *         or inclusion in the top.
     **/
    public String update(String id, long count, int maxsize) {
        int position = locations.getOrDefault(id, -1);
        if (position >= 0) {
            values[position] = count;
            return null;
        }
        resize(maxsize);
        if (size < values.length) {
            values[size] = count;
            locations.put(id, size);
            keys[size] = id;
            size++;
            return null;
        } else if (count <= values[size - 1]) {
            return id;
        } else {
            return replace(id, count, false);
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
        int position = locations.getOrDefault(key, -1);
        if (position >= 0) {
            return values[position];
        } else {
            return null;
        }
    }
}
