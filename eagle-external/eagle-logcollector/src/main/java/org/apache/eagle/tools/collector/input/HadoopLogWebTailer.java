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
package org.apache.eagle.tools.collector.input;

import com.typesafe.config.Config;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.eagle.tools.collector.OutputCollector;
import org.apache.eagle.tools.collector.input.impl.LogSecureSocketFactory;
import org.apache.eagle.tools.collector.input.impl.MemoryLogStateManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * TODO: 1. async request 2. set fetch timeout
 *
 * @link {org.apache.hadoop.hbase.http.HttpServer}
 *
 * @since 4/16/15
 */
public class HadoopLogWebTailer implements LogInput {

    static { LogSecureSocketFactory.init(); }

    private final static byte NEWLINE = "\n".getBytes()[0];
    private Logger LOG = LoggerFactory.getLogger(HadoopLogWebTailer.class);
    private Pattern logSplitPattern;
    private String logRootPathPattern;
    private OutputCollector outputCollector;

    public LogStateManager getLogStateManager() {
        return logStateManager;
    }

    // Set transient to avoid to be serialized
    private transient LogStateManager logStateManager;
    private transient HttpClient httpClient;

    private final static Map<String,Pattern> logFilePathPatternCache = Collections.synchronizedMap(new HashMap<String, Pattern>());

    private final static String STATE_TYPE_KEY ="stateType";
    private final static String STATE_PROPS_KEY ="stateProps";
    private final static String LOG_FILE_PATH_PATTERN_KEY ="logFilePathPattern";
    private final static String LOG_ROOT_PATH_PATTER_KEY ="logRootPathPattern";
    private final static String LOG_SPLIT_PATTERN_KEY ="logSplitPattern";
    private final static String INITIAL_LOG_LOOK_BEHIND_SIZE_IN_KBS_KEY ="initialLogLookBehindSizeInKbs";
    private final static String LOG_TYPE_KEY ="logType";
    private final static String HOST_NAME_KEY ="hostName";
    private final static String INFO_PORT_KEY ="infoPort";

    private Integer initialLogLookBehindSizeInKbs;
    private String logFilePathPattern;
    private String logType;
    private HostConfig hostConfig;

    /**
     * TODO: Change config to POJO
     * @param config
     * @param name
     */
    @Override
    public void init(Config config, String name) {
        if(name == null) {
            LOG = LoggerFactory.getLogger(HadoopLogWebTailer.class);
        }else{
            LOG = LoggerFactory.getLogger("HadoopLogWebTailer:"+name);
        }
        this.outputCollector = outputCollector;
        this.httpClient = new HttpClient();
        this.logStateManager =  new MemoryLogStateManagerImpl();
        this.logStateManager.prepare(config);
        if(hostConfig == null){
            if(config.hasPath(HOST_NAME_KEY) || config.hasPath(INFO_PORT_KEY)){
                hostConfig = new HostConfig();
                if(config.hasPath(HOST_NAME_KEY)){
                    hostConfig.setHostName(config.getString(HOST_NAME_KEY));
                }
                if(config.hasPath(INFO_PORT_KEY)){
                    hostConfig.setInfoPort(config.getInt(INFO_PORT_KEY));
                }
            }
        }
        if(config.hasPath(LOG_TYPE_KEY)){
            this.logType = config.getString(LOG_TYPE_KEY);
        }
        if(config.hasPath(LOG_FILE_PATH_PATTERN_KEY)) {
            this.logFilePathPattern = config.getString(LOG_FILE_PATH_PATTERN_KEY);
        } else {
            String sampleLogFilePathPattern = "\"(?i)/logs/(.*.log)\"";
            throw new IllegalStateException("Config key '"+LOG_FILE_PATH_PATTERN_KEY+"' not found, example: '"+sampleLogFilePathPattern+"'");
        }
        if(config.hasPath(LOG_ROOT_PATH_PATTER_KEY)) {
            this.logRootPathPattern = config.getString(LOG_ROOT_PATH_PATTER_KEY);
        } else {
            this.logRootPathPattern = "http://%hostname%:%infoport%/logs";
        }
        if(config.hasPath(INITIAL_LOG_LOOK_BEHIND_SIZE_IN_KBS_KEY)) {
            this.initialLogLookBehindSizeInKbs = config.getInt(INITIAL_LOG_LOOK_BEHIND_SIZE_IN_KBS_KEY);
        } else {
            this.initialLogLookBehindSizeInKbs = 1024;
        }
        String logSplitPatternStr;
        if(config.hasPath(LOG_SPLIT_PATTERN_KEY)) {
            logSplitPatternStr = config.getString(LOG_SPLIT_PATTERN_KEY);
            this.logSplitPattern = Pattern.compile(logSplitPatternStr, Pattern.MULTILINE);
        }else{
            logSplitPatternStr = "\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2},\\d{3}(.*)";
        }
        this.logSplitPattern = Pattern.compile(logSplitPatternStr, Pattern.MULTILINE);
    }

    @Override
    public void prepare(OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void next() throws IOException {
        this.collect(hostConfig);
    }

    private Pattern getLogFilePathPattern(HostConfig context) {
        String logFilePathPattern = this.logFilePathPattern;
        if(logFilePathPattern == null){
            throw new RuntimeException("[logFilePathPattern] is null");
        }
        Pattern pattern = logFilePathPatternCache.get(logFilePathPattern);
        if(pattern == null){
            String patternStr = context.format(logFilePathPattern);
            pattern = Pattern.compile(patternStr);
            logFilePathPatternCache.put(patternStr,pattern);
        }
        return pattern;
    }

    private static Map<String,String> logFileUrlCache = Collections.synchronizedMap(new HashMap<String, String>());
    private static volatile long logFileUrlCacheLastUpdateTime = System.currentTimeMillis();

    private String buildLogFileNameCacheKey(String logRootUrl){
        return String.format("%s/%s",logRootUrl,this.logFilePathPattern);
    }

    private String discoverCurrentLogFileFullUrl(HostConfig source, String logRootUrlPattern) throws IOException {
        String logRootUrl = source.format(logRootUrlPattern);

        // TODO: allow cache configurable, default: 30 mins
        if(System.currentTimeMillis() - logFileUrlCacheLastUpdateTime >= 30 * 60 * 1000){
            logFileUrlCache.clear();
            logFileUrlCacheLastUpdateTime = System.currentTimeMillis();
        }

        String logFileUrl = null;
        String logFileNameCacheKey = this.buildLogFileNameCacheKey(logRootUrl);
        String cachedLogFileUrl = logFileUrlCache.get(logFileNameCacheKey);

        if(cachedLogFileUrl == null) {
            GetMethod getMethod = new GetMethod(logRootUrl);
            try {
                LOG.info("Discovering log file name from: " + logRootUrl);
                this.httpClient.executeMethod(getMethod);
                String body = getMethod.getResponseBodyAsString();
                Matcher m = getLogFilePathPattern(source).matcher(body);
                if (m.find()) {
                    String path = m.group(1);
                    LOG.info(String.format("Found path matching pattern [%s]: %s",
                            this.logFilePathPattern, path));
                    if (logRootUrl.endsWith("/")) {
                        logFileUrl = source.format(logRootUrl + path);
                    } else {
                        logFileUrl = source.format(logRootUrl + "/" + path);
                    }
                } else {
                    LOG.warn(String.format("Not found path matching [%s] on [%s]",
                            this.logFilePathPattern, logRootUrl));
                }
            } catch (IOException e) {
                LOG.error("Failed to get url: " + logRootUrl+", due to: "+e.getMessage(),e);
                throw e;
            } finally {
                getMethod.releaseConnection();
            }

            LOG.info(String.format("Discovered log file url: [%s]", logFileUrl));
            logFileUrlCache.put(logFileNameCacheKey, logFileUrl);
        }else{
            logFileUrl = cachedLogFileUrl;
            if(LOG.isDebugEnabled()) LOG.debug(String.format("Cached log file url: [%s]", cachedLogFileUrl));
        }
        return logFileUrl;
    }

    /**
     * Tail of certain log file
     *
     * Thread safe
     *
     * @return crawled log content
     * @throws IOException
     */
    public void collect(HostConfig host) throws IOException {
        if(host == null) throw new NullPointerException("host is not set before being used");
        String logFileUrl = discoverCurrentLogFileFullUrl(host,logRootPathPattern);
        LOG.debug(String.format("Reading log content from: %s",logFileUrl));

        Long currentOffset = logStateManager.getLogOffset(logFileUrl);
        if(currentOffset == null){
            GetMethod getMethod = new GetMethod(logFileUrl);
            try {
                int statusCode = this.httpClient.executeMethod(getMethod);
                if (statusCode != 200) {
                    throw new IOException("status code is " + statusCode + ", expected: 200");
                }
                long contentLen = 0;
                Header contentLenHeader = getMethod.getResponseHeader("Content-Length");
                if (contentLenHeader != null) {
                    contentLen = Long.parseLong(contentLenHeader.getValue());
                }
                long offset = Math.max(0, contentLen - this.initialLogLookBehindSizeInKbs * 1024);
                LOG.info(String.format("Initializing log offset as [%d] for log file at %s with content-length [%d]",
                        offset, logFileUrl, contentLen));
                currentOffset = this.logStateManager.setLogOffset(logFileUrl, offset);
            } catch (IOException e) {
                throw e;
            } finally {
                getMethod.releaseConnection();
            }
        }

        GetMethod method = null;
        try {
            method = getLogContentByOffset(logFileUrl, currentOffset);
            if (wasRotated(method, logFileUrl)) {
                currentOffset = this.logStateManager.setLogOffset(logFileUrl, 0l);
                LOG.info(String.format("Log file [%s] seems to have rotated (server returned 416), resetting offset to 0",
                        logFileUrl));
                // release connection before creating new
                method.releaseConnection();
                method = getLogContentByOffset(logFileUrl, currentOffset);
                if (this.wasRotated(method, logFileUrl)) {
                    // release connection before creating new
                    method.releaseConnection();
                    throw new IOException("Could not get log file from server even after resetting logOffset to 0");
                }
            }
            byte[] data = method.getResponseBody(Integer.MAX_VALUE);
            if(data == null) throw new IOException(String.format("Body from [%s, %d-] is null",logFileUrl,currentOffset));

            int len  = data.length - 1;
            long newOffset = currentOffset + offsetOfLastNewLine(data);

            if(LOG.isDebugEnabled() && len > 0) {
                LOG.debug(String.format("Read bytes = %d, offset = [%d ~ %d]", len, currentOffset, newOffset));
            }
            else if(len > 0){
                LOG.info(String.format("Read bytes = %d, offset = [%d ~ %d]", len, currentOffset, newOffset));
            }

            if(len == 0) {
                Thread.sleep(3000);
                return;
            }

            LOG.debug(String.format("Updating logfile offset to [%d] for file %s", newOffset, logFileUrl));
            this.logStateManager.setLogOffset(logFileUrl, newOffset);

            String content = new String(data, "UTF-8");

            if (this.logSplitPattern == null) {
                LogTuple tuple = new LogTuple();
                tuple.setType(this.logType);
                tuple.setData(content);
                tuple.setOffset(currentOffset);
                if(outputCollector !=null) outputCollector.emit(tuple);
                if(LOG.isDebugEnabled()) {
                    LOG.info("Emmit {}", tuple);
                }
            } else {
                Matcher matcher = this.logSplitPattern.matcher(content);
                Long fetchOffset = currentOffset;
                while (matcher.find()) {
                    String split = matcher.group();
                    fetchOffset += split.getBytes().length;
                    LogTuple tuple = new LogTuple();
                    tuple.setType(this.logType);
                    tuple.setData(split);
                    tuple.setOffset(fetchOffset);
                    if(outputCollector !=null) outputCollector.emit(tuple);
                    if(LOG.isDebugEnabled()) {
                        LOG.info("Emmit {}", tuple);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to GET "+logFileUrl,e);
            throw e;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // make sure release connection
            if(method!=null) method.releaseConnection();
        }
    }

    private long offsetOfLastNewLine(byte[] bytes){
        for(int i = bytes.length - 1; i >= 0; i--){
            if(bytes[i] == NEWLINE){
                return i;
            }
        }
        return 0;
    }

    /**
     * Read latest log starting from [offset]
     *
     * <br/>
     * <b>Header:</b> "Range", "bytes=%d-".format(offset)
     *
     * @param url log url
     * @param startOffset starting bytes offset
     * @return tailed log content
     * @throws IOException
     */
    private GetMethod getLogContentByOffset(String url, Long startOffset) throws IOException {
        if(LOG.isDebugEnabled()) LOG.debug(String.format("Fetching Logfile from %s with range [%d-]", url, startOffset));
        GetMethod getMethod = new GetMethod(url);
        getMethod.addRequestHeader("Range", String.format("bytes=%d-",startOffset));
        getMethod.addRequestHeader("Accept-Ranges","bytes");

        int statusCode = this.httpClient.executeMethod(getMethod);
        if (statusCode != 200 && statusCode != 206 && statusCode != 416) {
            throw new IOException("couldn't get Compaction Metrics from URL: '" +
                    url + " (statusCode was: " + statusCode + ")");
        }
        return getMethod;
    }

    private boolean wasRotated(GetMethod getMethod,String url){
        if(getMethod.getStatusCode() == 416){
            LOG.info(String.format("Log file [%s] seems to have rotated (statusCode = 416)", url));
            getMethod.releaseConnection();
            return true;
        }
        Header contentRangeHeader = getMethod.getResponseHeader("Content-Range");
        if(contentRangeHeader == null){
            if(LOG.isDebugEnabled()) LOG.debug("Content-Range is null, so should not be rotated yet");
            return false;
        }
        String contentRange = contentRangeHeader.getValue();
        String rangeValue = StringUtils.substringBetween(contentRange, "bytes", "/").trim();
        if(rangeValue.equals("*")){
            LOG.info(String.format("Log file [%s] seems to have rotated (RangeValue is %s)",url,rangeValue));
            return true;
        }
        return false;
    }
}