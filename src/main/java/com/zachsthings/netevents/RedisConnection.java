package com.zachsthings.netevents;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.bukkit.Bukkit;
import org.bukkit.event.Event;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisConnection {
    public static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final byte[] CHANNEL = "NetEvents".getBytes(UTF_8);

    private JedisPool mPool;
    private NetEventsPlugin mPlugin;

    public RedisConnection(NetEventsPlugin plugin, String host, int port, String password) {
        if (password == null || password.isEmpty()) {
            mPool = new JedisPool(new JedisPoolConfig(), host, port, 0);
        } else {
            mPool = new JedisPool(new JedisPoolConfig(), host, port, 0, password);
        }
        
        mPlugin = plugin;
    }
    
    public void close() {
        mPool.destroy();
    }

    public Future<Void> listen() {
        final DataHandler wrapper = new DataHandler();
        
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                Jedis jedis = mPool.getResource();
                try {
                    mPlugin.debug("Subscribing to 'NetEvents'");
                    jedis.subscribe(wrapper, CHANNEL);
                    mPlugin.debug("REDIS 'NetEvents' channel terminated");
                } catch (Exception e) {
                    e.printStackTrace();
                    mPool.returnBrokenResource(jedis);
                    return;
                }
                mPool.returnResource(jedis);
            }
        });
        
        thread.start();
        
        return wrapper;
    }
    
    private void handle(final Event event) {
        mPlugin.debug("Handling event " + event);
        Bukkit.getScheduler().runTask(mPlugin, new Runnable() {
            @Override
            public void run() {
                Bukkit.getPluginManager().callEvent(event);
            }
        });
    }
    
    public void broadcast(Event event) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        try {
            ObjectOutputStream out = new ObjectOutputStream(stream);
            out.writeShort(Bukkit.getPort());
            out.writeObject(event);
        } catch (IOException e) {
            mPlugin.debug("Error while sending " + event + ": " + e.getMessage());
            e.printStackTrace();
            return;
        }

        Jedis jedis = mPool.getResource();
        try {
            jedis.publish(CHANNEL, stream.toByteArray());
        } catch (Exception e) {
            e.printStackTrace();
            mPool.returnBrokenResource(jedis);
            return;
        }
        mPool.returnResource(jedis);
    }

    private class DataHandler extends BinaryJedisPubSub implements Future<Void> {
        private CountDownLatch mLatch;

        public DataHandler() {
            mLatch = new CountDownLatch(1);
        }

        @Override
        public void onMessage(byte[] channel, byte[] data) {
            if (channel != CHANNEL) {
                return;
            }
            
            try {
                ByteArrayInputStream stream = new ByteArrayInputStream(data);
                ObjectInputStream in = new ObjectInputStream(stream);

                int source = in.readUnsignedShort();

                if (source == Bukkit.getPort()) {
                    return;
                }
                
                Object object = in.readObject();
                if (!(object instanceof Event)) {
                    mPlugin.debug("Received '" + object + "' which is not an event");
                    return;
                }
                
                handle((Event)object);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                mPlugin.debug(e.getMessage());
                return;
            }
        }

        @Override
        public void onPMessage(byte[] paramArrayOfByte1, byte[] paramArrayOfByte2, byte[] paramArrayOfByte3) {
        }

        @Override
        public void onSubscribe(byte[] channel, int paramInt) {
            mLatch.countDown();
        }

        @Override
        public void onUnsubscribe(byte[] channel, int paramInt) {
        }

        @Override
        public void onPUnsubscribe(byte[] paramArrayOfByte, int paramInt) {
        }

        @Override
        public void onPSubscribe(byte[] paramArrayOfByte, int paramInt) {
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return mLatch.getCount() == 0;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            mLatch.await();
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            mLatch.await(timeout, unit);
            return null;
        }
    }
}
