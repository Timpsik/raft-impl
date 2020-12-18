package com.raft.requests;

/**
 * Response for the {@link ReadRequest}
 * <p>
 * Contains the integer value for asked variable
 */
public class ReadResponse extends RaftResponse {

    /**
     * Stored value for asked variable
     */
    private int value;

    public ReadResponse(String leaderAddress, boolean success, int value) {
        super(success, leaderAddress);
        this.value = value;
    }

    public ReadResponse(String leaderAddress, boolean success, ErrorCause cause) {
        super(success, leaderAddress, cause);
    }

    public int getValue() {
        return value;
    }

}
