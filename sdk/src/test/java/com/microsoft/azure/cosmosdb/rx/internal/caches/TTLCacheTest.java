/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.cosmosdb.rx.internal.caches;

import org.apache.commons.lang3.builder.ToStringExclude;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

public class TTLCacheTest {


    @Test(expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenDurationIsNull() {
        TTLCache.of(1, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldReturnIllegalWhenValueIsNegative() {
        TTLCache.of(-1, TimeUnit.DAYS);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldReturnIllegalWhenValueIsZero() {
        TTLCache.of(0, TimeUnit.DAYS);
    }

    @Test
    public void shouldCreateInstance() {
        assertNotNull(TTLCache.of(1L, TimeUnit.DAYS));
        assertNotNull(TTLCache.of(1L, TimeUnit.HOURS));
        assertNotNull(TTLCache.of(1L, TimeUnit.MINUTES));
        assertNotNull(TTLCache.of(1L, TimeUnit.SECONDS));
        assertNotNull(TTLCache.of(1L, TimeUnit.MICROSECONDS));
        assertNotNull(TTLCache.of(1L, TimeUnit.NANOSECONDS));
        assertNotNull(TTLCache.of(1L, TimeUnit.MILLISECONDS));
    }

    @Test
    public void shouldPut() {
        Map<String, Integer> map = TTLCache.of(1, TimeUnit.NANOSECONDS);
        assertNull(map.put("one", 1));
        assertNotNull(map.put("one", 1));
    }

    @Test
    public void shouldGet() throws InterruptedException {
        Map<String, Integer> map = TTLCache.of(2, TimeUnit.MILLISECONDS);
        map.put("one", 1);
        assertNotNull(map.get("one"));
        TimeUnit.MILLISECONDS.sleep(5L);
        assertNull(map.get("one"));
    }

    @Test
    public void shouldGetSize() {
        Map<String, Integer> map = TTLCache.of(2, TimeUnit.MILLISECONDS);
        assertEquals(0, map.size());
        map.put("one", 1);
        assertEquals(1, map.size());
        map.put("two", 2);
        assertEquals(2, map.size());
    }

    @Test
    public void shouldGetEmpty() {
        Map<String, Integer> map = TTLCache.of(2, TimeUnit.MILLISECONDS);
        assertTrue(map.isEmpty());
        map.put("one", 1);
        assertFalse(map.isEmpty());
    }

    @Test
    public void shouldContains() throws InterruptedException {
        Map<String, Integer> map = TTLCache.of(2, TimeUnit.MILLISECONDS);
        map.put("one", 1);
        assertTrue(map.containsKey("one"));
        TimeUnit.MILLISECONDS.sleep(5L);
        assertFalse(map.containsKey("one"));
    }

    @Test
    public void shouldContainsValue() {
        Map<String, Integer> map = TTLCache.of(2, TimeUnit.MILLISECONDS);
        map.put("one", 1);
        assertTrue(map.containsValue(1));
        assertFalse(map.containsValue(2));
    }

    @Test
    public void shouldRemove() {
        Map<String, Integer> map = TTLCache.of(2, TimeUnit.MILLISECONDS);
        map.put("one", 1);
        assertTrue(map.containsKey("one"));
        map.remove("one");
        assertFalse(map.containsKey("one"));
    }

    @Test
    public void shouldClear() {
        Map<String, Integer> map = TTLCache.of(2, TimeUnit.MILLISECONDS);
        map.put("one", 1);
        map.put("two", 1);
        assertFalse(map.isEmpty());
        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenPutAllIsNull() {
        Map<String, Integer> map = TTLCache.of(2, TimeUnit.MILLISECONDS);
        map.putAll(null);
    }

    @Test
    public void shouldPutAll() throws InterruptedException {
        Map<String, Integer> map = TTLCache.of(2, TimeUnit.MILLISECONDS);
        map.putAll(Collections.singletonMap("one", 1));
        assertTrue(map.containsKey("one"));
        TimeUnit.MILLISECONDS.sleep(5L);
        assertFalse(map.containsKey("one"));
    }

    @Test
    public void shouldReturnKeySet() throws InterruptedException {
        Map<String, Integer> map = TTLCache.of(2, TimeUnit.MILLISECONDS);
        assertTrue(map.keySet().isEmpty());
        map.put("one", 1);
        map.put("two", 2);
        assertFalse(map.keySet().isEmpty());
        assertEquals(2, map.keySet().size());
        TimeUnit.MILLISECONDS.sleep(5L);
        map.put("four", 4);
        assertFalse(map.keySet().isEmpty());
        assertEquals(1, map.keySet().size());
    }

    @Test
    public void shouldReturnValues() throws InterruptedException {
        Map<String, Integer> map = TTLCache.of(2, TimeUnit.MILLISECONDS);
        assertTrue(map.values().isEmpty());
        map.put("one", 1);
        map.put("two", 2);
        assertFalse(map.values().isEmpty());
        assertEquals(2, map.values().size());
        TimeUnit.MILLISECONDS.sleep(5L);
        map.put("four", 4);
        assertFalse(map.values().isEmpty());
        assertEquals(1, map.values().size());
    }

    @Test
    public void shouldReturnEntrySet() throws InterruptedException {
        Map<String, Integer> map = TTLCache.of(2, TimeUnit.MILLISECONDS);
        assertTrue(map.entrySet().isEmpty());
        map.put("one", 1);
        map.put("two", 2);
        assertFalse(map.entrySet().isEmpty());
        assertEquals(2, map.entrySet().size());
        TimeUnit.MILLISECONDS.sleep(5L);
        map.put("four", 4);
        assertFalse(map.entrySet().isEmpty());
        assertEquals(1, map.entrySet().size());
    }
}