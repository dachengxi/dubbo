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

package org.apache.dubbo.remoting.transport;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.FrameworkServiceRepository;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.dubbo.common.BaseServiceMetadata.keyWithoutGroup;

/**
 * 编解码器的辅助类
 */
public class CodecSupport {
    private static final Logger logger = LoggerFactory.getLogger(CodecSupport.class);

    /**
     * 缓存序列化方式
     */
    private static Map<Byte, Serialization> ID_SERIALIZATION_MAP = new HashMap<Byte, Serialization>();

    /**
     * 缓存序列化方式的名字
     */
    private static Map<Byte, String> ID_SERIALIZATIONNAME_MAP = new HashMap<Byte, String>();
    private static Map<String, Byte> SERIALIZATIONNAME_ID_MAP = new HashMap<String, Byte>();
    // Cache null object serialize results, for heartbeat request/response serialize use.
    private static Map<Byte, byte[]> ID_NULLBYTES_MAP = new HashMap<Byte, byte[]>();

    private static final ThreadLocal<byte[]> TL_BUFFER = ThreadLocal.withInitial(() -> new byte[1024]);

    static {
        // 获取Serialization的扩展加载器
        ExtensionLoader<Serialization> extensionLoader = FrameworkModel.defaultModel().getExtensionLoader(Serialization.class);、

        // 获取Serialization的所有的扩展实现，下面循环将所有扩展实现的相关信息放到缓存中
        Set<String> supportedExtensions = extensionLoader.getSupportedExtensions();
        for (String name : supportedExtensions) {
            Serialization serialization = extensionLoader.getExtension(name);
            byte idByte = serialization.getContentTypeId();
            if (ID_SERIALIZATION_MAP.containsKey(idByte)) {
                logger.error("Serialization extension " + serialization.getClass().getName()
                        + " has duplicate id to Serialization extension "
                        + ID_SERIALIZATION_MAP.get(idByte).getClass().getName()
                        + ", ignore this Serialization extension");
                continue;
            }
            ID_SERIALIZATION_MAP.put(idByte, serialization);
            ID_SERIALIZATIONNAME_MAP.put(idByte, name);
            SERIALIZATIONNAME_ID_MAP.put(name, idByte);
        }
    }

    private CodecSupport() {
    }

    /**
     * 根据id获取序列化的实现
     * @param id
     * @return
     */
    public static Serialization getSerializationById(Byte id) {
        return ID_SERIALIZATION_MAP.get(id);
    }

    /**
     * 根据名字获取序列化实现的id
     * @param name
     * @return
     */
    public static Byte getIDByName(String name) {
        return SERIALIZATIONNAME_ID_MAP.get(name);
    }

    /**
     * 根据URL中的参数获取序列化的实现
     * @param url
     * @return
     */
    public static Serialization getSerialization(URL url) {
        return url.getOrDefaultFrameworkModel().getExtensionLoader(Serialization.class).getExtension(
                url.getParameter(Constants.SERIALIZATION_KEY, Constants.DEFAULT_REMOTING_SERIALIZATION));
    }

    /**
     * 获取序列化方式
     * @param url
     * @param id
     * @return
     * @throws IOException
     */
    public static Serialization getSerialization(URL url, Byte id) throws IOException {
        Serialization result = getSerializationById(id);
        if (result == null) {
            throw new IOException("Unrecognized serialize type from consumer: " + id);
        }
        return result;
    }

    /**
     * 反序列化数据
     * @param url
     * @param is
     * @param proto
     * @return
     * @throws IOException
     */
    public static ObjectInput deserialize(URL url, InputStream is, byte proto) throws IOException {
        // 获取序列化方式
        Serialization s = getSerialization(url, proto);

        // 使用具体的序列化实现类进行反序列化
        return s.deserialize(url, is);
    }

    /**
     * Get the null object serialize result byte[] of Serialization from the cache,
     * if not, generate it first.
     *
     * @param s Serialization Instances
     * @return serialize result of null object
     *
     * 获取null的序列化结果
     */
    public static byte[] getNullBytesOf(Serialization s) {
        return ID_NULLBYTES_MAP.computeIfAbsent(s.getContentTypeId(), k -> {
            //Pre-generated Null object bytes
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] nullBytes = new byte[0];
            try {
                ObjectOutput out = s.serialize(null, baos);
                out.writeObject(null);
                out.flushBuffer();
                nullBytes = baos.toByteArray();
                baos.close();
            } catch (Exception e) {
                logger.warn("Serialization extension " + s.getClass().getName() + " not support serializing null object, return an empty bytes instead.");
            }
            return nullBytes;
        });
    }

    /**
     * Read all payload to byte[]
     *
     * @param is
     * @return
     * @throws IOException
     *
     * 获取流中的数据
     */
    public static byte[] getPayload(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = getBuffer(is.available());
        int len;
        while ((len = is.read(buffer)) > -1) {
            baos.write(buffer, 0, len);
        }
        baos.flush();
        return baos.toByteArray();
    }

    private static byte[] getBuffer(int size) {
        byte[] bytes = TL_BUFFER.get();
        if (size <= bytes.length) {
            return bytes;
        }
        return new byte[size];
    }

    /**
     * Check if payload is null object serialize result byte[] of serialization
     *
     * @param payload
     * @param proto
     * @return
     */
    public static boolean isHeartBeat(byte[] payload, byte proto) {
        return Arrays.equals(payload, getNullBytesOf(getSerializationById(proto)));
    }

    public static void checkSerialization(FrameworkServiceRepository serviceRepository, String path, String version, Byte id) throws IOException {
        List<URL> urls = serviceRepository.lookupRegisteredProviderUrlsWithoutGroup(keyWithoutGroup(path, version));
        if (CollectionUtils.isEmpty(urls)) {
            throw new IOException("Service " + path + " with version " + version + " not found, invocation rejected.");
        } else {
            boolean match = false;
            for (URL url : urls) {
                String serializationName = url.getParameter(org.apache.dubbo.remoting.Constants.SERIALIZATION_KEY, Constants.DEFAULT_REMOTING_SERIALIZATION);
                Byte localId = SERIALIZATIONNAME_ID_MAP.get(serializationName);
                if (localId != null && localId.equals(id)) {
                    match = true;
                }
            }
            if(!match) {
                throw new IOException("Unexpected serialization id:" + id + " received from network, please check if the peer send the right id.");
            }
        }

    }


}
