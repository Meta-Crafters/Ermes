package com.glyart.ermes.redis;

import com.glyart.ermes.common.channels.IDataCompressor;
import com.glyart.ermes.common.connections.IConnection;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class RedisConnection implements IConnection<RedisMessagingChannel, JedisPool> {

    private final RedisCredentials credentials;
    private JedisPool pool;

    private RedisConnection(RedisCredentials credentials) {
        this.credentials = credentials;
    }

    public static RedisConnection create(RedisCredentials credentials) {
        return new RedisConnection(credentials);
    }

    @Override
    public void connect() {
        final String password = this.credentials.password();
        if (password.isEmpty()) {
            this.pool = new JedisPool(new JedisPoolConfig(), this.credentials.hostname(), this.credentials.port(), Protocol.DEFAULT_TIMEOUT);
            return;
        }

        this.pool = new JedisPool(new JedisPoolConfig(), this.credentials.hostname(), this.credentials.port(), Protocol.DEFAULT_TIMEOUT, this.credentials.password());
    }

    @Override
    public void disconnect() {
        this.pool.close();
    }

    @Override
    public RedisMessagingChannel createChannel(String name, IDataCompressor compressor, boolean canConsumeMessages, int compressionThreshold) {
        final RedisMessagingChannel channel = new RedisMessagingChannel(name, compressor, canConsumeMessages, compressionThreshold);
        channel.connect(this);
        return channel;
    }

    @Override
    public JedisPool getConnection() {
        return this.pool;
    }

}
