/**
 * Copyright (C) 2014 zml (netevents@zachsthings.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zachsthings.netevents;

import org.bukkit.Bukkit;
import org.bukkit.event.Event;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import java.util.logging.Level;

/**
 * Main class for NetEvents
 */
public class NetEventsPlugin extends JavaPlugin {
    /**
     * Number of event UUID's to keep to prevent duplicate events. Greater number potentially decreases duplicate events received.
     */
    public static final int EVENT_CACHE_COUNT = 5000;

    private boolean debugMode;
    private RedisConnection redisConnection;
    private UUID serverUUID;

    @Override
    public void onEnable() {
        this.saveDefaultConfig();
        getConfig().options().copyDefaults(true);
        reloadConfig();
        
        serverUUID = UUID.randomUUID();

        try {
            connect();
            redisConnection.listen();
        } catch (Exception e) {
            getLogger().log(Level.SEVERE, "Unable to connect to redis", e);
            getServer().getPluginManager().disablePlugin(this);
            return;
        }
        getCommand("netevents").setExecutor(new StatusCommand(this));
    }

    @Override
    public void onDisable() {
        try {
            close();
        } catch (Exception e) {
            getLogger().log(Level.SEVERE, "Unable to properly disconnect network connections", e);
        }
    }
    
    @Override
    public void reloadConfig() {
        super.reloadConfig();
        
        debugMode = getConfig().getBoolean("debug", false);
    }

    /**
     * Reload the configuration for this plugin.
     *
     * @throws IOException When an error occurs while working with connections
     */
    public void reload() throws IOException {
        close();
        reloadConfig();
        connect();
    }

    private void close(){
        redisConnection.close();
    }

    private void connect(){
        redisConnection = new RedisConnection(this, getConfig().getString("redis.host", "localhost"), getConfig().getInt("redis.port", 6379), getConfig().getString("redis.password", ""));
        debug("Redis connection established");
    }

    /**
     * Set whether debug logging is enabled.
     *
     * @see #debug(String)
     * @param debug The value to set debug logging to
     */
    public void setDebugMode(boolean debug) {
        debugMode = debug;
    }

    /**
     *
     * @return Whether or not debug logging is enabled
     */
    public boolean hasDebugMode() {
        return debugMode;
    }

    /**
     * Logs a debug message only if NetEvents has debug mode enabled ({@link #hasDebugMode()}).
     *
     * @param message The message to log
     */
    protected void debug(String message) {
        if (hasDebugMode()) {
            getLogger().warning("[DEBUG] " + message);
        }
    }

    /**
     * Calls the passed event on this server and forwards it to all
     * connected servers to be called remotely.
     *
     * Events must be serializable.
     * Remote servers will ignore events they do not know the class of.
     *
     * @param event The event to call
     * @param <T> The event type
     * @return The event (same as passed, just here for utility)
     */
    public <T extends Event & Serializable> T callEvent(T event) {
        debug("Calling event " + event);
        redisConnection.broadcast(event);
        Bukkit.getPluginManager().callEvent(event);
        return event;
    }
    
    /**
     * Returns a persistent unique ID for this server.
     * Useful for identifying this server in the network.
     *
     * @return a unique id for this server.
     */
    public UUID getServerUUID() {
        return serverUUID;
    }
}
