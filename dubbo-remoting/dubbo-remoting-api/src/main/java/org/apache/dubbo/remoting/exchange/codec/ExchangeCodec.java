/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.exchange.codec;

import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.common.io.StreamUtils;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.Cleanable;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.buffer.ChannelBufferInputStream;
import org.apache.dubbo.remoting.buffer.ChannelBufferOutputStream;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.telnet.codec.TelnetCodec;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.remoting.transport.ExceedPayloadLimitException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * ExchangeCodec.
 *
 * 信息交换层的编解码器
 */
public class ExchangeCodec extends TelnetCodec {

    // header length.
    // header长度，16个字节
    protected static final int HEADER_LENGTH = 16;

    // magic header.
    // 魔数，0-15位
    protected static final short MAGIC = (short) 0xdabb;

    // 魔数的高8位，0-7位
    protected static final byte MAGIC_HIGH = Bytes.short2bytes(MAGIC)[0];

    // 魔数的低8位，8-15位
    protected static final byte MAGIC_LOW = Bytes.short2bytes(MAGIC)[1];

    // message flag.
    // 消息标识：是请求还是响应，第16位
    protected static final byte FLAG_REQUEST = (byte) 0x80;

    // 消息标识：消息是单向还是双向，第17位
    protected static final byte FLAG_TWOWAY = (byte) 0x40;

    // 消息标识：消息是否是事件消息，第18位
    protected static final byte FLAG_EVENT = (byte) 0x20;

    // 序列化类型标识，用来标识使用的是哪一种序列化方式，19-23位
    protected static final int SERIALIZATION_MASK = 0x1f;
    private static final Logger logger = LoggerFactory.getLogger(ExchangeCodec.class);

    public Short getMagicCode() {
        return MAGIC;
    }

    /**
     * 编码
     * @param channel
     * @param buffer
     * @param msg
     * @throws IOException
     */
    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object msg) throws IOException {
        // 对请求进行编码
        if (msg instanceof Request) {
            encodeRequest(channel, buffer, (Request) msg);
        } else if (msg instanceof Response) {
            // 对响应进行编码
            encodeResponse(channel, buffer, (Response) msg);
        } else {
            // 其他类型的编码
            super.encode(channel, buffer, msg);
        }
    }

    /**
     * 解码
     * @param channel
     * @param buffer
     * @return
     * @throws IOException
     */
    @Override
    public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {
        int readable = buffer.readableBytes();
        byte[] header = new byte[Math.min(readable, HEADER_LENGTH)];
        buffer.readBytes(header);
        // 解码
        return decode(channel, buffer, readable, header);
    }

    /**
     * 解码
     * @param channel
     * @param buffer
     * @param readable
     * @param header
     * @return
     * @throws IOException
     */
    @Override
    protected Object decode(Channel channel, ChannelBuffer buffer, int readable, byte[] header) throws IOException {
        // check magic number.
        // 检查魔数
        if (readable > 0 && header[0] != MAGIC_HIGH
                || readable > 1 && header[1] != MAGIC_LOW) {

            // header的长度
            int length = header.length;
            if (header.length < readable) {
                header = Bytes.copyOf(header, readable);
                buffer.readBytes(header, length, readable - length);
            }
            for (int i = 1; i < header.length - 1; i++) {
                if (header[i] == MAGIC_HIGH && header[i + 1] == MAGIC_LOW) {
                    buffer.readerIndex(buffer.readerIndex() - header.length + i);
                    header = Bytes.copyOf(header, i);
                    break;
                }
            }
            return super.decode(channel, buffer, readable, header);
        }

        // check length.
        // 接收到的数据比header长度小，需要等待更多的数据到来
        if (readable < HEADER_LENGTH) {
            return DecodeResult.NEED_MORE_INPUT;
        }

        // get data length.
        // 数据长度
        int len = Bytes.bytes2int(header, 12);

        // When receiving response, how to exceed the length, then directly construct a response to the client.
        // see more detail from https://github.com/apache/dubbo/issues/7021.
        // 服务端响应的数据长度超过客户端最大限制
        Object obj = finishRespWhenOverPayload(channel, len, header);
        if (null != obj) {
            return obj;
        }

        checkPayload(channel, len);

        int tt = len + HEADER_LENGTH;
        if (readable < tt) {
            return DecodeResult.NEED_MORE_INPUT;
        }

        // limit input stream.
        ChannelBufferInputStream is = new ChannelBufferInputStream(buffer, len);

        try {
            // 解码body
            return decodeBody(channel, is, header);
        } finally {
            if (is.available() > 0) {
                try {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Skip input stream " + is.available());
                    }
                    StreamUtils.skipUnusedStream(is);
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * 解码body
     * @param channel
     * @param is
     * @param header
     * @return
     * @throws IOException
     */
    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
        // 消息标识以及序列化类型
        byte flag = header[2], proto = (byte) (flag & SERIALIZATION_MASK);

        // get request id.
        // 请求ID
        long id = Bytes.bytes2long(header, 4);

        // 解码响应的body
        if ((flag & FLAG_REQUEST) == 0) {
            // decode response.
            Response res = new Response(id);

            // 是否是事件
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(true);
            }

            // get status.
            // 响应状态
            byte status = header[3];
            res.setStatus(status);
            try {
                // 成功状态
                if (status == Response.OK) {
                    Object data;
                    if (res.isEvent()) {
                        byte[] eventPayload = CodecSupport.getPayload(is);
                        if (CodecSupport.isHeartBeat(eventPayload, proto)) {
                            // heart beat response data is always null;
                            // 心跳事件的body是null
                            data = null;
                        } else {
                            // 反序列化并解码其他的事件
                            data = decodeEventData(channel, CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload), proto), eventPayload);
                        }
                    } else {
                        // 反序列化并解码响应的数据
                        data = decodeResponseData(channel, CodecSupport.deserialize(channel.getUrl(), is, proto), getRequestData(id));
                    }
                    res.setResult(data);
                } else {
                    res.setErrorMessage(CodecSupport.deserialize(channel.getUrl(), is, proto).readUTF());
                }
            } catch (Throwable t) {
                res.setStatus(Response.CLIENT_ERROR);
                res.setErrorMessage(StringUtils.toString(t));
            }
            return res;
        }
        // 解码请求的body
        else {
            // decode request.
            Request req = new Request(id);

            // 协议版本
            req.setVersion(Version.getProtocolVersion());

            // 消息是单向还是双向
            req.setTwoWay((flag & FLAG_TWOWAY) != 0);

            // 是否是事件
            if ((flag & FLAG_EVENT) != 0) {
                req.setEvent(true);
            }
            try {
                Object data;
                // 事件请求
                if (req.isEvent()) {
                    byte[] eventPayload = CodecSupport.getPayload(is);
                    // 心跳
                    if (CodecSupport.isHeartBeat(eventPayload, proto)) {
                        // heart beat response data is always null;
                        // 心跳事件的body是null
                        data = null;
                    } else {
                        // 反序列化并解码事件数据
                        data = decodeEventData(channel, CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload), proto), eventPayload);
                    }
                } else {
                    // 反序列化并解码请求数据
                    data = decodeRequestData(channel, CodecSupport.deserialize(channel.getUrl(), is, proto));
                }
                req.setData(data);
            } catch (Throwable t) {
                // bad request
                req.setBroken(true);
                req.setData(t);
            }
            return req;
        }
    }

    /**
     * 获取请求的数据，请求的时候会将请求数据缓存到DefaultFuture中，
     * 使用请求ID作为唯一标识。
     * @param id
     * @return
     */
    protected Object getRequestData(long id) {
        // 根据请求ID从缓存中获取DefaultFuture
        DefaultFuture future = DefaultFuture.getFuture(id);
        if (future == null) {
            return null;
        }

        // 获取到请求数据
        Request req = future.getRequest();
        if (req == null) {
            return null;
        }
        return req.getData();
    }

    /**
     * 对请求进行编码
     * @param channel
     * @param buffer
     * @param req
     * @throws IOException
     */
    protected void encodeRequest(Channel channel, ChannelBuffer buffer, Request req) throws IOException {
        // 获取序列化方式，默认是hessian2
        Serialization serialization = getSerialization(channel, req);

        // header.
        // header长度，16个字节
        byte[] header = new byte[HEADER_LENGTH];

        // set magic number.
        // 魔数，2个字节，0-15位
        Bytes.short2bytes(MAGIC, header);

        // set request and serialization flag.
        // 请求标识、序列化方式，各占1位，是请求还是响应在第16位；序列化方式在第19-23位
        header[2] = (byte) (FLAG_REQUEST | serialization.getContentTypeId());

        // 消息是单向还是双向，第17位
        if (req.isTwoWay()) {
            header[2] |= FLAG_TWOWAY;
        }

        // 消息是否是事件消息，第18位
        if (req.isEvent()) {
            header[2] |= FLAG_EVENT;
        }

        // set request id.
        // 设置请求ID，32-95位
        Bytes.long2bytes(req.getId(), header, 4);

        // encode request data.
        /*
            下面是对请求的数据进行编码，
            - 第96-127位是序列化后的数据的长度，4个字节
            - 128位之后是序列化后的数据
         */
        int savedWriteIndex = buffer.writerIndex();
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);
        ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer);

        if (req.isHeartbeat()) {
            // heartbeat request data is always null
            bos.write(CodecSupport.getNullBytesOf(serialization));
        } else {
            // 序列化数据
            ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
            if (req.isEvent()) {
                // 编码事件数据
                encodeEventData(channel, out, req.getData());
            } else {
                // 编码请求数据，实现是在DubboCodec中
                encodeRequestData(channel, out, req.getData(), req.getVersion());
            }
            out.flushBuffer();
            if (out instanceof Cleanable) {
                ((Cleanable) out).cleanup();
            }
        }

        bos.flush();
        bos.close();

        // 序列化后的数据长度
        int len = bos.writtenBytes();

        // 默认最大数据长度8M，需要检查一下
        checkPayload(channel, len);

        // 将数据的长度写到header中，是96位-127位
        Bytes.int2bytes(len, header, 12);

        // write
        // 重新调整ChannelBuffer的写入位置到写入header的位置
        buffer.writerIndex(savedWriteIndex);

        // 写入header数据
        buffer.writeBytes(header); // write header.

        // 重新调整ChannelBuffer的正确位置
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
    }

    /**
     * 编码响应
     * @param channel
     * @param buffer
     * @param res
     * @throws IOException
     */
    protected void encodeResponse(Channel channel, ChannelBuffer buffer, Response res) throws IOException {
        int savedWriteIndex = buffer.writerIndex();
        try {
            // 获取序列化方式，默认是hessian2
            Serialization serialization = getSerialization(channel, res);

            // header.
            // header长度，16个字节
            byte[] header = new byte[HEADER_LENGTH];

            // set magic number.
            // 魔数，2个字节，0-15位
            Bytes.short2bytes(MAGIC, header);


            // set request and serialization flag.
            // 请求标识、序列化方式，各占1位，是请求还是响应在第16位；序列化方式在第19-23位
            header[2] = serialization.getContentTypeId();

            // 消息是否是事件消息，第18位
            if (res.isHeartbeat()) {
                header[2] |= FLAG_EVENT;
            }

            // set response status.
            // 响应状态，在第24-31位
            byte status = res.getStatus();
            header[3] = status;

            // set request id.
            // 设置请求ID，32-95位
            Bytes.long2bytes(res.getId(), header, 4);

            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);
            ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer);

            // encode response data or error message.
            // 编码响应数据或者错误信息
            if (status == Response.OK) {
                // 心跳响应
                if(res.isHeartbeat()){
                    // heartbeat response data is always null
                    bos.write(CodecSupport.getNullBytesOf(serialization));
                }else {
                    // 序列化相应数据
                    ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
                    if (res.isEvent()) {
                        // 编码事件响应
                        encodeEventData(channel, out, res.getResult());
                    } else {
                        // 编码响应数据
                        encodeResponseData(channel, out, res.getResult(), res.getVersion());
                    }
                    out.flushBuffer();
                    if (out instanceof Cleanable) {
                        ((Cleanable) out).cleanup();
                    }
                }
            } else {
                // 编码错误消息
                ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
                out.writeUTF(res.getErrorMessage());
                out.flushBuffer();
                if (out instanceof Cleanable) {
                    ((Cleanable) out).cleanup();
                }
            }

            bos.flush();
            bos.close();

            // 序列化后的数据长度
            int len = bos.writtenBytes();

            // 默认最大数据长度8M，需要检查一下
            checkPayload(channel, len);

            // 将数据的长度写到header中，是96位-127位
            Bytes.int2bytes(len, header, 12);

            // write
            // 重新调整ChannelBuffer的写入位置到写入header的位置
            buffer.writerIndex(savedWriteIndex);

            // 写入header数据
            buffer.writeBytes(header); // write header.

            // 重新调整ChannelBuffer的正确位置
            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
        } catch (Throwable t) {
            // clear buffer
            buffer.writerIndex(savedWriteIndex);
            // send error message to Consumer, otherwise, Consumer will wait till timeout.
            if (!res.isEvent() && res.getStatus() != Response.BAD_RESPONSE) {
                Response r = new Response(res.getId(), res.getVersion());
                r.setStatus(Response.BAD_RESPONSE);

                if (t instanceof ExceedPayloadLimitException) {
                    logger.warn(t.getMessage(), t);
                    try {
                        r.setErrorMessage(t.getMessage());
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn("Failed to send bad_response info back: " + t.getMessage() + ", cause: " + e.getMessage(), e);
                    }
                } else {
                    // FIXME log error message in Codec and handle in caught() of IoHanndler?
                    logger.warn("Fail to encode response: " + res + ", send bad_response info instead, cause: " + t.getMessage(), t);
                    try {
                        r.setErrorMessage("Failed to send response: " + res + ", cause: " + StringUtils.toString(t));
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn("Failed to send bad_response info back: " + res + ", cause: " + e.getMessage(), e);
                    }
                }
            }

            // Rethrow exception
            if (t instanceof IOException) {
                throw (IOException) t;
            } else if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof Error) {
                throw (Error) t;
            } else {
                throw new RuntimeException(t.getMessage(), t);
            }
        }
    }

    /**
     * 解码数据
     * @param in
     * @return
     * @throws IOException
     */
    @Override
    protected Object decodeData(ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    /**
     * 解码请求数据
     * @param in
     * @return
     * @throws IOException
     */
    protected Object decodeRequestData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    /**
     * 解码响应数据
     * @param in
     * @return
     * @throws IOException
     */
    protected Object decodeResponseData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    @Override
    protected void encodeData(ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    /**
     * 编码事件数据
     * @param out
     * @param data
     * @throws IOException
     */
    private void encodeEventData(ObjectOutput out, Object data) throws IOException {
        out.writeEvent(data);
    }

    /**
     * 编码心跳数据
     * @param out
     * @param data
     * @throws IOException
     */
    @Deprecated
    protected void encodeHeartbeatData(ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    /**
     * 编码请求数据
     * @param out
     * @param data
     * @throws IOException
     */
    protected void encodeRequestData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    /**
     * 编码响应数据
     * @param out
     * @param data
     * @throws IOException
     */
    protected void encodeResponseData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    /**
     * 解码数据
     * @param channel
     * @param in
     * @return
     * @throws IOException
     */
    @Override
    protected Object decodeData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(channel, in);
    }

    /**
     * 解码事件数据
     * @param channel
     * @param in
     * @param eventBytes
     * @return
     * @throws IOException
     */
    protected Object decodeEventData(Channel channel, ObjectInput in, byte[] eventBytes) throws IOException {
        try {
            if (eventBytes != null) {
                int dataLen = eventBytes.length;
                int threshold = ConfigurationUtils.getSystemConfiguration(channel.getUrl().getScopeModel()).getInt("deserialization.event.size", 50);
                if (dataLen > threshold) {
                    throw new IllegalArgumentException("Event data too long, actual size " + threshold + ", threshold " + threshold + " rejected for security consideration.");
                }
            }
            return in.readEvent();
        } catch (IOException | ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Decode dubbo protocol event failed.", e));
        }
    }

    /**
     * 解码请求数据
     * @param channel
     * @param in
     * @return
     * @throws IOException
     */
    protected Object decodeRequestData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    /**
     * 解码响应数据
     * @param channel
     * @param in
     * @return
     * @throws IOException
     */
    protected Object decodeResponseData(Channel channel, ObjectInput in) throws IOException {
        return decodeResponseData(in);
    }

    /**
     * 解码响应数据
     * @param channel
     * @param in
     * @param requestData
     * @return
     * @throws IOException
     */
    protected Object decodeResponseData(Channel channel, ObjectInput in, Object requestData) throws IOException {
        return decodeResponseData(channel, in);
    }

    @Override
    protected void encodeData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data);
    }

    /**
     * 编码事件数据
     * @param channel
     * @param out
     * @param data
     * @throws IOException
     */
    private void encodeEventData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    /**
     * 编码心跳数据
     * @param channel
     * @param out
     * @param data
     * @throws IOException
     */
    @Deprecated
    protected void encodeHeartbeatData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeHeartbeatData(out, data);
    }

    /**
     * 编码请求数据
     * @param channel
     * @param out
     * @param data
     * @throws IOException
     */
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    /**
     * 编码响应数据
     * @param channel
     * @param out
     * @param data
     * @throws IOException
     */
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(out, data);
    }

    /**
     * 编码请求数据
     * @param channel
     * @param out
     * @param data
     * @param version
     * @throws IOException
     */
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        encodeRequestData(out, data);
    }

    /**
     * 编码响应数据
     * @param channel
     * @param out
     * @param data
     * @param version
     * @throws IOException
     */
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        encodeResponseData(out, data);
    }

    private Object finishRespWhenOverPayload(Channel channel, long size, byte[] header) {
        int payload = getPayload(channel);
        boolean overPayload = isOverPayload(payload, size);
        if (overPayload) {
            long reqId = Bytes.bytes2long(header, 4);
            byte flag = header[2];
            if ((flag & FLAG_REQUEST) == 0) {
                Response res = new Response(reqId);
                if ((flag & FLAG_EVENT) != 0) {
                    res.setEvent(true);
                }
                res.setStatus(Response.CLIENT_ERROR);
                String errorMsg = "Data length too large: " + size + ", max payload: " + payload + ", channel: " + channel;
                logger.error(errorMsg);
                res.setErrorMessage(errorMsg);
                return res;
            }
        }
        return null;
    }
}
