package com.raft.server.conf;

/**
 * Configuration for the server
 */
public final class Configuration {

    /**
     * Maximum possible value for election timeout
     */
    public static int maxElectionTimeout = 300;

    /**
     * Minimum possible value for election timeout
     */
    public static int minElectionTimeout = 150;
}
