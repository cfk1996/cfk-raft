package com.github.chenfeikun.raft.core;

/**
 * @desciption: Entry
 * @CreateTime: 2019-03-21
 * @author: chenfeikun
 */
public class Entry {

    public static final int POS_OFFSET = 4 + 4 + 8 + 8;
    public static final int HEADER_SIZE = POS_OFFSET + 8 + 4 + 4 + 4;
    public static final int BODY_OFFSET = HEADER_SIZE + 4;

    private int magic;
    private int size;
    private long term;
    private long index;
    private long pos;
    private int channel;
    private int chainCrc;
    private int bodyCrc;
    private byte[] body;

    public int getMagic() {
        return magic;
    }

    public void setMagic(int magic) {
        this.magic = magic;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public int getChannel() {
        return channel;
    }

    public void setChannel(int channel) {
        this.channel = channel;
    }

    public int getChainCrc() {
        return chainCrc;
    }

    public void setChainCrc(int chainCrc) {
        this.chainCrc = chainCrc;
    }

    public int getBodyCrc() {
        return bodyCrc;
    }

    public void setBodyCrc(int bodyCrc) {
        this.bodyCrc = bodyCrc;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int computSizeInBytes() {
        size = HEADER_SIZE + 4 + body.length;
        return size;
    }

    @Override
    public boolean equals(Object entry) {
        if (entry == null || !(entry instanceof Entry)) {
            return false;
        }
        Entry other = (Entry) entry;
        if (this.size != other.size
                || this.magic != other.magic
                || this.index != other.index
                || this.term != other.term
                || this.channel != other.channel
                || this.pos != other.pos) {
            return false;
        }
        if (body == null) {
            return other.body == null;
        }

        if (other.body == null) {
            return false;
        }
        if (body.length != other.body.length) {
            return false;
        }
        for (int i = 0; i < body.length; i++) {
            if (body[i] != other.body[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int h = 1;
        h = prime * h + size;
        h = prime * h + magic;
        h = prime * h + (int) index;
        h = prime * h + (int) term;
        h = prime * h + channel;
        h = prime * h + (int) pos;
        if (body != null) {
            for (int i = 0; i < body.length; i++) {
                h = prime * h + body[i];
            }
        } else {
            h = prime * h;
        }
        return h;
    }
}
