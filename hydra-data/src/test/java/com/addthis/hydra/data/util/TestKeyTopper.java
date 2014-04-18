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

import java.nio.charset.Charset;

import com.google.common.base.Charsets;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestKeyTopper {

    /**
     * The commented out line are to illustrate that the continuation
     * prefix is "10xxxxxx".
     */
    @Test
    public void testContinuationByte() {
        Charset charset = Charsets.UTF_8;
        assertTrue(((KeyTopper.CONTINUATION_BYTE & 0xff) >>> 6) == 2);
        for (int i = Character.MIN_VALUE; i < Character.MAX_VALUE; i++) {
            String s = Character.toString((char) i);
            byte[] encoded = s.getBytes(charset);
            //assertTrue("At " + i, ((encoded[0] & 0xff) >>> 6) != 0);
            //assertTrue("At " + i, ((encoded[0] & 0xff) >>> 6) != 1);
            assertTrue("At " + i, ((encoded[0] & 0xff) >>> 6) != 2);
            //assertTrue("At " + i, ((encoded[0] & 0xff) >>> 6) != 3);
        }
    }

    @Test
    public void testIncrementNoEviction() {
        KeyTopper topper = new KeyTopper();

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        Long one = (long) 1;

        for (int i = 0; i < 100; i++) {
            assertEquals(one, topper.get(Integer.toString(i)));
        }

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        Long two = (long) 2;

        for (int i = 0; i < 100; i++) {
            assertEquals(two, topper.get(Integer.toString(i)));
        }

    }

    @Test
    public void testIncrementWithEviction() {
        KeyTopper topper = new KeyTopper();

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        Long one = (long) 1;

        for (int i = 0; i < 100; i++) {
            assertEquals(one, topper.get(Integer.toString(i)));
        }

        String evicted = topper.increment(Integer.toString(101), 100);

        assertNotNull(evicted);
        assertNotNull(topper.get(Integer.toString(101)));
        assertNull(topper.get(evicted));

        evicted = topper.increment(Integer.toString(101), 100);

        assertNull(evicted);

        assertEquals(new Long(3), topper.get(Integer.toString(101)));


    }

    @Test
    public void testWeightedIncrementWithEviction() {
        KeyTopper topper = new KeyTopper();

        for (int i = 0; i < 100; i++) {
            topper.increment(Integer.toString(i), 100);
        }

        assertEquals(100, topper.size());

        Long one = (long) 1;

        for (int i = 0; i < 100; i++) {
            assertEquals(one, topper.get(Integer.toString(i)));
        }

        String evicted = topper.increment(Integer.toString(101), 2, 100);

        assertNotNull(evicted);

        assertNull(topper.get(evicted));

        assertEquals(new Long(3), topper.get(Integer.toString(101)));
    }

}