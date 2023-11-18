package org.crazycake.shiro.common;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.crazycake.shiro.IRedisManager;
import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Abstract class of RedisManager.
 */
public abstract class WorkAloneRedisManager implements IRedisManager {

    /**
     * We are going to operate redis by acquiring Jedis object.
     * The subclass should realizes the way to get Jedis objects by implement the getJedis().
     * @return Jedis
     */
    protected abstract Jedis getJedis();

    /**
     * Default value of count.
     */
    protected static final int DEFAULT_COUNT = 100;

    /**
     * The number of elements returned at every iteration.
     */
    private int count = DEFAULT_COUNT;

    /**
     * JedisPoolConfig used to initialize JedisPool.
     */
    private JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    private GenericObjectPoolConfig<Connection> genericObjectPoolConfig = new GenericObjectPoolConfig<>();

    /**
     * get value from redis
     * @param key key
     * @return value
     */
    @Override
    public byte[] get(byte[] key) {
        if (key == null) {
            return null;
        }
        byte[] value;
        try (Jedis jedis = getJedis()) {
            value = jedis.get(key);
        }
        return value;
    }

    /**
     * set
     * @param key key
     * @param value value
     * @param expireTime expire time in second
     * @return value
     */
    @Override
    public byte[] set(byte[] key, byte[] value, long expireTime) {
        if (key == null) {
            return null;
        }
        try (Jedis jedis = getJedis()) {
            jedis.set(key, value);
            // -1 and 0 is not a valid expire time in Jedis
            if (expireTime > 0) {
                jedis.expire(key, expireTime);
            }
        }
        return value;
    }

    /**
     * Delete a key-value pair.
     * @param key key
     */
    @Override
    public void del(byte[] key) {
        if (key == null) {
            return;
        }
        try (Jedis jedis = getJedis()) {
            jedis.del(key);
        }
    }

    /**
     * Return the size of redis db.
     * @param pattern key pattern
     * @return key-value size
     */
    @Override
    public Long dbSize(byte[] pattern) {
        long dbSize = 0L;
        try (Jedis jedis = getJedis()) {
            ScanParams params = new ScanParams();
            params.count(count);
            params.match(pattern);
            byte[] cursor = ScanParams.SCAN_POINTER_START_BINARY;
            ScanResult<byte[]> scanResult;
            do {
                scanResult = jedis.scan(cursor, params);
                List<byte[]> results = scanResult.getResult();
                for (byte[] result : results) {
                    dbSize++;
                }
                cursor = scanResult.getCursorAsBytes();
            } while (scanResult.getCursor().compareTo(ScanParams.SCAN_POINTER_START) > 0);
        }
        return dbSize;
    }

    /**
     * Return all the keys of Redis db. Filtered by pattern.
     * @param pattern key pattern
     * @return key set
     */
    public Set<byte[]> keys(byte[] pattern) {
        Set<byte[]> keys = new HashSet<byte[]>();

        try (Jedis jedis = getJedis()) {
            ScanParams params = new ScanParams();
            params.count(count);
            params.match(pattern);
            byte[] cursor = ScanParams.SCAN_POINTER_START_BINARY;
            ScanResult<byte[]> scanResult;
            do {
                scanResult = jedis.scan(cursor, params);
                keys.addAll(scanResult.getResult());
                cursor = scanResult.getCursorAsBytes();
            } while (scanResult.getCursor().compareTo(ScanParams.SCAN_POINTER_START) > 0);
        }
        return keys;

    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public JedisPoolConfig getJedisPoolConfig() {
        return jedisPoolConfig;
    }

    public void setJedisPoolConfig(JedisPoolConfig jedisPoolConfig) {
        this.jedisPoolConfig = jedisPoolConfig;
    }

    public GenericObjectPoolConfig<Connection> getGenericObjectPoolConfig() {
        return genericObjectPoolConfig;
    }

    public void setGenericObjectPoolConfig(GenericObjectPoolConfig<Connection> genericObjectPoolConfig) {
        this.genericObjectPoolConfig = genericObjectPoolConfig;
    }
}
