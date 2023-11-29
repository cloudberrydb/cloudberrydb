package cn.hashdata.dlagent.api.io;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A serializable object for transporting a byte array through the Bridge
 * framework
 */
public class BufferWritable implements Writable {

    byte[] buf;
    int length;

    /**
     * Constructs a BufferWritable. Copies the buffer reference and not the
     * actual bytes. This class is used when we intend to transport a buffer
     * through the Bridge framework without copying the data each time the
     * buffer is passed between the Bridge objects.
     *
     * @param inBuf buffer reference
     */
    public BufferWritable(byte[] inBuf) {
        this(inBuf, inBuf.length);
    }

    /**
     * Constructs a BufferWritable. Copies the buffer reference (not the
     * actual bytes) and the length of bytes.
     * This class is used when we intend to transport a buffer through the
     * Bridge framework without copying the data each time the buffer is passed
     * between the Bridge objects.
     *
     * @param inBuf  buffer reference
     * @param length the length of data within the buffer
     */
    public BufferWritable(byte[] inBuf, int length) {
        this.buf = inBuf;
        this.length = length;
    }

    /**
     * Serializes the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOutput</code> to serialize this object into.
     * @throws IOException if the buffer was not set
     */
    @Override
    public void write(DataOutput out) throws IOException {
        if (buf == null)
            throw new IOException("BufferWritable was not set");
        out.write(buf, 0, length);
    }

    /**
     * Deserializes the fields of this object from <code>in</code>.
     * <p>
     * For efficiency, implementations should attempt to re-use storage in the
     * existing object where possible.
     * </p>
     *
     * @param in <code>DataInput</code> to deserialize this object from
     * @throws UnsupportedOperationException this function is not supported
     */
    @Override
    public void readFields(DataInput in) {
        throw new UnsupportedOperationException(
                "BufferWritable.readFields() is not implemented");
    }

    /**
     * Appends given app's buffer to existing buffer.
     * <br>
     * Not efficient - requires copying both this and the appended buffer.
     *
     * @param app buffer to append
     */
    public void append(byte[] app) {
        if (buf == null) {
            buf = app;
            length = buf.length;
            return;
        }
        if (app == null) {
            return;
        }

        byte[] newbuf = new byte[buf.length + app.length];
        System.arraycopy(buf, 0, newbuf, 0, buf.length);
        System.arraycopy(app, 0, newbuf, buf.length, app.length);
        buf = newbuf;
        length = newbuf.length;
    }
}
