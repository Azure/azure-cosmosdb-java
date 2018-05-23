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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;

/**
 *A Map implementation that expires based on TTL {@link TTLCache#of(long, TimeUnit)}
 * @param <K> the key type
 * @param <V> the value type
 */
public class TTLCache<K, V> implements Map<K, V> {

    private final Map<K, V> store = new ConcurrentHashMap<>();
    private final Map<K, Long> timestamps = new ConcurrentHashMap<>();
    private final long ttl;

    private TTLCache(long ttl) {
        this.ttl = ttl;
    }

    @Override
    public V get(Object key) {
        V value = this.store.get(key);

        if (value != null && checkExpired(key)) {
            return null;
        } else {
            return value;
        }
    }

    @Override
    public V put(K key, V value) {
        timestamps.put(key, System.nanoTime());
        return store.put(key, value);
    }

    @Override
    public int size() {
        return store.size();
    }

    @Override
    public boolean isEmpty() {
        return store.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        boolean containsKey = store.containsKey(key);
        return containsKey ? !checkExpired(key) : containsKey;
    }

    @Override
    public boolean containsValue(Object value) {
        return store.containsValue(value);
    }

    @Override
    public V remove(Object key) {
        timestamps.remove(key);
        return store.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        Objects.requireNonNull(map, "map is required");
        map.entrySet().forEach(this::put);
    }

    @Override
    public void clear() {
        timestamps.clear();
        store.clear();
    }

    @Override
    public Set<K> keySet() {
        clearExpired();
        return unmodifiableSet(store.keySet());
    }

    @Override
    public Collection<V> values() {
        clearExpired();
        return unmodifiableCollection(store.values());
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        clearExpired();
        return unmodifiableSet(store.entrySet());
    }

    private void clearExpired() {
        store.keySet().stream().forEach(this::checkExpired);
    }

    private void put(Entry<? extends K, ? extends V> entry) {
        this.put(entry.getKey(), entry.getValue());
    }

    private boolean checkExpired(Object key) {
        if (isExpired(key)) {
            remove(key);
            return true;
        }
        return false;
    }

    private boolean isExpired(Object key) {
        return (System.nanoTime() - timestamps.get(key)) > this.ttl;
    }

    /**
     * Creates a {@link Map} that expires values from the TTL defined.
     * The value is represented by nanoseconds, so any amount lower than one nanosecond will come around to one.
     * @param value the value
     * @param timeUnit the unit
     * @param <K> the key type
     * @param <V> the value type
     * @return a new {@link TTLCache} instance
     * @throws NullPointerException when timeUnit is null
     * @throws IllegalArgumentException when value is negative or zero
     */
    public static <K,V> Map<K, V> of(long value, TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit, "timeUnit is required");
        if(value <= 0) {
            throw new IllegalArgumentException("The value to TTL must be greater than zero");
        }
        return new TTLCache<>(timeUnit.toNanos(value));
    }

}
