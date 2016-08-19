/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.common.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * TrustAllSSLSocketFactory.
 */
public class TrustAllSSLSocketFactory extends SSLSocketFactory {
    private SSLSocketFactory socketFactory;

    /**
     * TrustAllSSLSocketFactory.
     */
    public TrustAllSSLSocketFactory() {
        try {
            SSLContext ctx = SSLContext.getInstance("SSL");
            ctx.init(null, new TrustManager[] {new TrustAnyTrustManager() {
            }}, null);
            socketFactory = ctx.getSocketFactory();
        } catch (Exception ex) {
            ex.printStackTrace(System.err);  /* handle exception */
        }
    }

    public static SocketFactory getDefault() {
        return new TrustAllSSLSocketFactory();
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return socketFactory.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return socketFactory.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket(Socket socket, String string, int port, boolean bln) throws IOException {
        return socketFactory.createSocket(socket, string, port, bln);
    }

    @Override
    public Socket createSocket(String string, int port) throws IOException, UnknownHostException {
        return socketFactory.createSocket(string, port);
    }

    @Override
    public Socket createSocket(String string, int port, InetAddress ia, int port1) throws IOException, UnknownHostException {
        return socketFactory.createSocket(string, port, ia, port1);
    }

    @Override
    public Socket createSocket(InetAddress ia, int port) throws IOException {
        return socketFactory.createSocket(ia, port);
    }

    @Override
    public Socket createSocket(InetAddress ia, int port, InetAddress ia1, int port1) throws IOException {
        return socketFactory.createSocket(ia, port, ia1, port1);
    }

    private static class TrustAnyTrustManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String authType) {
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String authType) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }
    }
}

