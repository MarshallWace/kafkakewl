/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.logback;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.pattern.ThrowableHandlingConverter;
import ch.qos.logback.classic.pattern.ThrowableProxyConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.contrib.json.JsonLayoutBase;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.pattern.PatternLayoutBase;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Custom JSON Layout for Logback
 */
public class JsonLayout extends JsonLayoutBase<ILoggingEvent> {

    private static final String TIMESTAMP_ATTR_NAME = "timestampUtc";
    private static final String LEVEL_ATTR_NAME = "level";
    private static final String THREAD_ATTR_NAME = "thread";
    private static final String LOGGER_ATTR_NAME = "logger";
    private static final String FORMATTED_MESSAGE_ATTR_NAME = "message";
    private static final String EXCEPTION_ATTR_NAME = "exception";
    private static final String VERSION_ATTR_NAME = "version";
    private static final String APPLICATION_ATTR_NAME = "application";
    private static final String INSTANCE_ATTR_NAME = "instance";
    private static final String HOSTNAME_ATTR_NAME = "hostname";
    private static final String USERNAME_ATTR_NAME = "username";

    // used to format exceptions
    private ThrowableHandlingConverter throwableProxyConverter;

    // static application informations to set in the application's code
    private static String APPLICATION_VALUE = null;
    private static String VERSION_VALUE = null;
    private static String INSTANCE_VALUE = null;

    // application informations set via logback.xml (will take precedence over the static values above)
    private Fields metadata;
    private Fields custom;
    private String application;
    private String version;
    private String instance;
    private boolean includeMDC;

    // a field definition in logback.xml
    public static class Field {
        private String name;
        private String value;
        private boolean allowEmpty;

        private PatternLayoutBase<ILoggingEvent> layout;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public boolean getAllowEmpty() {
            return this.allowEmpty;
        }

        public void setAllowEmpty(boolean value) {
            this.allowEmpty = value;
        }

        public void startLayout(Context context) {
            this.layout = new PatternLayout();
            this.layout.setContext(context);
            this.layout.setPattern(getValue());
            this.layout.setPostCompileProcessor(null);
            this.layout.start();
        }

        public void stopLayout() {
            this.layout.stop();
        }

        public String doLayout(ILoggingEvent event) {
            return this.layout.doLayout(event);
        }
    }


    // a fields list definition in logback.xml
    public static class Fields {
        private List<Field> fields;
        private String fieldName = "metadata";

        public Fields() {
            this.fields = new ArrayList<>();
        }

        public List<Field> getFields() {
            return fields;
        }

        public void addField(Field field) {
            fields.add(field);
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public void startLayout(Context context) {
            for (Field field: fields) {
                field.startLayout(context);
            }
        }

        public void stopLayout() {
            for (Field field: fields) {
                field.stopLayout();
            }
        }
    }

    // set static information about the application. To use in the application's code. Logback
    // will use these values unless they are set in logback.xml (which will take priority).
    public static void setApplication(String application, String instance, String version) {
        APPLICATION_VALUE = application;
        VERSION_VALUE = version;
        INSTANCE_VALUE = instance;
    }

    // constructor
    public JsonLayout() {
        super();
        this.throwableProxyConverter = new ThrowableProxyConverter();
    }

    // Setter for parameter in logback.xml
    public void setMetadata(Fields metadata) {
        this.metadata = metadata;
        this.custom.setFieldName("metadata");
    }

    // Setter for parameter in logback.xml
    public void setCustomFields(Fields custom) {
        this.custom = custom;
        this.custom.setFieldName("customFields");
    }

    // Setter for parameter in logback.xml
    public void setApplication(String application) {
        this.application = application;
    }

    // Setter for parameter in logback.xml
    public void setVersion(String version) {
        this.version = version;
    }

    // Setter for parameter in logback.xml
    public void setInstance(String instance) {
        this.instance = instance;
    }

    public boolean isIncludeMDC() {
        return includeMDC;
    }

    public void setIncludeMDC(boolean includeMDC) {
        this.includeMDC = includeMDC;
    }

    // support for exception formatting
    @Override
    public void start() {
        this.throwableProxyConverter.start();
        this.custom.startLayout(super.getContext());
        super.start();
    }

    // support for exception formatting
    @Override
    public void stop() {
        super.stop();
        this.custom.stopLayout();
        this.throwableProxyConverter.stop();
    }

    // the formatting logic
    @Override
    protected Map toJsonMap(ILoggingEvent event) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (event == null) return map;

        // timestamp formatted using the options of ch.qos.logback.contrib.json.classic.JsonLayout
        String timeString = formatTimestamp(event.getTimeStamp());
        map.put(TIMESTAMP_ATTR_NAME, timeString);

        // use either instance values set via logback.xml or static values initialised with setApplication
        if (application != null)
            map.put(APPLICATION_ATTR_NAME, application);
        else if (APPLICATION_VALUE != null)
            map.put(APPLICATION_ATTR_NAME, APPLICATION_VALUE);
        if (version != null)
            map.put(VERSION_ATTR_NAME, version);
        else if (VERSION_VALUE != null)
            map.put(VERSION_ATTR_NAME, VERSION_VALUE);
        if (instance != null)
            map.put(INSTANCE_ATTR_NAME, instance);
        else if (INSTANCE_VALUE != null)
            map.put(INSTANCE_ATTR_NAME, INSTANCE_VALUE);

        // hostname and username
        try {
            map.put(HOSTNAME_ATTR_NAME, java.net.InetAddress.getLocalHost().getHostName());
        } catch (Exception ignore) {
        }
        map.put(USERNAME_ATTR_NAME, System.getProperty("user.name"));

        // metadata
        try {
            if (metadata != null) {
                Map<String, Object> metaMap = new HashMap<>();
                for (Field f : metadata.getFields()) {
                    metaMap.put(f.getName(), f.getValue());
                }
                map.put(metadata.getFieldName(), metaMap);
            }
        } catch (Exception ignore) {
        }

        // custom fields
        try {
            if (custom != null) {
                for (Field f : custom.getFields()) {
                    String fieldValue = f.doLayout(event);
                    if (!fieldValue.isEmpty() || f.getAllowEmpty()) {
                        map.put(f.getName(), fieldValue);
                    }
                }
            }
        } catch (Exception ignore) {
        }

        // mdc fields
        if (includeMDC) {
            map.putAll(event.getMDCPropertyMap());
        }

        // standard logging fields
        map.put(LEVEL_ATTR_NAME, String.valueOf(event.getLevel()));
        map.put(THREAD_ATTR_NAME, event.getThreadName());
        map.put(LOGGER_ATTR_NAME, event.getLoggerName());
        map.put(FORMATTED_MESSAGE_ATTR_NAME, event.getFormattedMessage());

        // exception
        try {
            IThrowableProxy throwableProxy = event.getThrowableProxy();
            if (throwableProxy != null) {
                String ex = throwableProxyConverter.convert(event);
                if (ex != null && !ex.equals("")) {
                    map.put(EXCEPTION_ATTR_NAME, ex);
                }
            }
        } catch (Exception ignore) {
        }

        // extra key/values passed as AbstractMap.SimpleEntry.
        // They may have been included with {} in the formatted message, or not.
        try {
            if (event.getArgumentArray() != null) {
                for (Object o : event.getArgumentArray()) {
                    if (o instanceof Map.Entry) {
                        Map.Entry e = (Map.Entry) o;
                        map.put(e.getKey().toString(), e.getValue());
                    }
                }
            }
        } catch (Exception ignore) {
        }

        return map;
    }
}