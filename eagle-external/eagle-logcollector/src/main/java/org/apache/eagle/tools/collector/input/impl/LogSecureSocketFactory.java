/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.tools.collector.input.impl;

import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.HttpClientError;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.SSLProtocolSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.net.*;

/**
 * Eagle SecureSocketFactory
 *
 * @link http://hc.apache.org/httpclient-3.x/sslguide.html
 * @link http://svn.apache.org/viewvc/httpcomponents/oac.hc3x/trunk/src/contrib/org/apache/commons/httpclient/contrib/ssl/EasySSLProtocolSocketFactory.java?view=markup
 */
public class LogSecureSocketFactory extends SSLProtocolSocketFactory {
    /** Log object for this class. */
    private static final Logger LOG = LoggerFactory.getLogger(LogSecureSocketFactory.class);
    private SSLContext sslcontext = null;

    public LogSecureSocketFactory() {
        super();
    }

    @Override
    public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException, UnknownHostException {
        return this.getSSLContext().getSocketFactory().createSocket(socket, host, port, autoClose);
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
        return this.getSSLContext().getSocketFactory().createSocket(host, port);
    }

    @Override
    public Socket createSocket(
            String host,
            int port,
            InetAddress localAddress,
            int localPort,
            HttpConnectionParams params) throws IOException, UnknownHostException, ConnectTimeoutException {
        if(params == null){
            throw new IllegalArgumentException("Parameters may not be null");
        }
        int timeout = params.getConnectionTimeout();
        SocketFactory socketFactory = getSSLContext().getSocketFactory();

        if(timeout == 0){
            return socketFactory.createSocket(host,port,localAddress,localPort);
        }else{
            Socket socket = socketFactory.createSocket();
            SocketAddress localaddr = new InetSocketAddress(localAddress,localPort);
            SocketAddress remoteaddr = new InetSocketAddress(host,port);
            socket.bind(localaddr);
            socket.connect(remoteaddr,timeout);
            return socket;
        }
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress clientHost, int clientPort) throws IOException, UnknownHostException {
        return this.getSSLContext().getSocketFactory().createSocket(host, port, clientHost, clientPort);
    }

    @Override
    public boolean equals(Object obj) {
        return ((obj != null) && obj.getClass().equals(LogSecureSocketFactory.class));
    }

    @Override
    public int hashCode() {
        return LogSecureSocketFactory.class.hashCode();
    }

    private static SSLContext createEagleSSLContext() {
        try {
            SSLContext context = SSLContext.getInstance("SSL");
            context.init(null,new TrustManager[] {new LogCollectorX509TrustManager()},null);
            return context;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new HttpClientError(e.toString());
        }
    }

    private SSLContext getSSLContext() {
        if (this.sslcontext == null) {
            this.sslcontext = createEagleSSLContext();
        }
        return this.sslcontext;
    }

    private static boolean isInit= false;
    public static void init(){
        if(!isInit) {
            LogSecureSocketFactory secureSocketFactory = new LogSecureSocketFactory();
            LOG.info("Registering protocol [https] with ["+secureSocketFactory.toString()+"]");
            Protocol.registerProtocol("https", new Protocol("https", secureSocketFactory, 443));
            isInit = true;
        }
    }
}