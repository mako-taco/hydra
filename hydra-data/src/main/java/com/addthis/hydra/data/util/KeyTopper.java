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

import com.addthis.codec.Codec;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;

public final class KeyTopper implements Codec.Codable {

    public KeyTopper() {
        values = new long[0];
        locations = new ObjectIntOpenHashMap<>(0);
        names = new String[0];
        size = 0;
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
     * values are stored in reverse sorted order.
     * locations assign strings to positions in the values array.
     * names is used to associate positions back into their keys.
     * size represents the number of items currently in the object.
     */
    private long[] values;
    private ObjectIntOpenHashMap<String> locations;
    private String[] names;
    private int size;
    
    @Override
    public String toString() {
        return "topper(" + Arrays.toString(values) + ")";
    }

    public KeyTopper init() {
        return this;
    }

    /**
     * returns the list sorted by greatest to least count.
     */
    public Map.Entry<String, Long>[] getSortedEntries() {
        Map.Entry<String, Long> e[] = new Map.Entry[size];
        for(int i = 0; i < size; i++) {
            e[i] = new AbstractMap.SimpleImmutableEntry<>(names[size - i - 1], values[size - i - 1]);
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
        ObjectIntOpenHashMap<String> newLocations = new ObjectIntOpenHashMap<>(newsize);
        String[] newNames = new String[newsize];
        System.arraycopy(values, 0, newValues, 0, size);
        System.arraycopy(names, 0, newNames, 0, size);
        newLocations.putAll(locations);
        values = newValues;
        names = newNames;
        locations = newLocations;
    }


    public String increment(String id, int maxsize) {
        return increment(id, 1, maxsize);
    }

    /**
     * Adds 'ID' the top N if: 1) there are more empty slots or 2) count >
     * smallest top count in the list
     * This one increments weight
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
            names[size] = id;
            locations.put(id, size);
            size++;
            return null;
        }
        position = selectMinElement();
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
            return true;
        } else {
            return false;
        }
    }

    /**
      * Adds 'ID' the top N if: 1) there are more empty slots or 2) count >
      * smallest top count in the list
      *
      * @param id
      * @param count
      * @return element dropped from top or null if accepted into top with no
      *         drops. returns the offered key if it was rejected for update
      *         or inclusion in the top.
      */
    public String update(String id, long count, int maxsize) {
        int position = locations.getOrDefault(id, -1);
        if (position >= 0) {
            values[size] = count;
            return null;
        }
        resize(maxsize);
        if (size < values.length) {
            values[size] = count;
            locations.put(id, size);
            names[size] = id;
            size++;
            return null;
        } else if (count <= values[size - 1]) {
            return id;
        } else {
            String dropped = names[size - 1];

            return dropped;
        }
    }

    public int size() {
        return size;
    }
}
