package com.g414.st9.proto.service.helper;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.http.HttpHeaders;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.RolloverFileOutputStream;
import org.eclipse.jetty.util.StringUtil;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.log.Log;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class ExtendedHttpRequestLogV20110917 extends AbstractLifeCycle
        implements RequestLog {
    private final ObjectMapper mapper = new ObjectMapper();
    private final DateTimeFormatter dateFormat = ISODateTimeFormat
            .basicDateTime().withZone(DateTimeZone.UTC);

    private String _filename;
    private boolean _append;
    private int _retainDays;
    private boolean _closeOut;

    private transient OutputStream _out;
    private transient OutputStream _fileOut;
    private transient Writer _writer;

    /* ------------------------------------------------------------ */
    /**
     * Create request log object with default settings.
     */
    public ExtendedHttpRequestLogV20110917() {
        _append = true;
        _retainDays = 31;
    }

    /* ------------------------------------------------------------ */
    /**
     * Create request log object with specified output file name.
     * 
     * @param filename
     *            the file name for the request log. This may be in the format
     *            expected by {@link RolloverFileOutputStream}
     */
    public ExtendedHttpRequestLogV20110917(String filename) {
        _append = true;
        _retainDays = 31;
        setFilename(filename);
    }

    /* ------------------------------------------------------------ */
    /**
     * Set the output file name of the request log. The file name may be in the
     * format expected by {@link RolloverFileOutputStream}.
     * 
     * @param filename
     *            file name of the request log
     * 
     */
    public void setFilename(String filename) {
        if (filename != null) {
            filename = filename.trim();
            if (filename.length() == 0) {
                filename = null;
            }
        }

        _filename = filename;
    }

    /* ------------------------------------------------------------ */
    /**
     * Retrieve the output file name of the request log.
     * 
     * @return file name of the request log
     */
    public String getFilename() {
        return _filename;
    }

    /* ------------------------------------------------------------ */
    /**
     * Retrieve the file name of the request log with the expanded date wildcard
     * if the output is written to the disk using
     * {@link RolloverFileOutputStream}.
     * 
     * @return file name of the request log, or null if not applicable
     */
    public String getDatedFilename() {
        if (_fileOut instanceof RolloverFileOutputStream) {
            return ((RolloverFileOutputStream) _fileOut).getDatedFilename();
        }

        return null;
    }

    /* ------------------------------------------------------------ */
    /**
     * Writes the request and response information to the output stream.
     * 
     * @see org.eclipse.jetty.server.RequestLog#log(org.eclipse.jetty.server.Request,
     *      org.eclipse.jetty.server.Response)
     */
    public void log(Request request, Response response) {
        try {
            if (_fileOut == null) {
                return;
            }

            Map<String, Object> out = new LinkedHashMap<String, Object>();

            out.put("_", 17);

            Long t1 = (Long) request
                    .getAttribute(ExtendedRequestFilter.T1_NANOS);
            long t2 = System.nanoTime();

            if (t1 != null) {
                out.put("d", dateFormat.print(t1 / 1000000));
            }

            String uuid = (String) response
                    .getHeader(ExtendedRequestFilter.X_REQUEST_ID);
            if (uuid != null) {
                out.put("r", uuid);
            }

            String addr = request.getHeader(HttpHeaders.X_FORWARDED_FOR);
            if (addr == null) {
                addr = request.getRemoteAddr();
            }

            out.put("i", addr);
            out.put("h", request.getServerName());
            out.put("m", request.getMethod());
            out.put("u", request.getUri().toString());
            out.put("p", request.getProtocol());
            out.put("s", response.getStatus());
            out.put("z", response.getContentCount());

            String referer = request.getHeader(HttpHeaders.REFERER);
            if (referer != null) {
                out.put("f", referer);
            }

            String agent = request.getHeader(HttpHeaders.USER_AGENT);
            if (referer != null) {
                out.put("a", agent);
            }

            if (t1 != null) {
                out.put("t", t2 - t1);
            }

            if (request.getHeader("X-User-ID") != null) {
                out.put("u", request.getHeader("X-User-ID"));
            }

            if (request.getHeader("X-Session-ID") != null) {
                out.put("n", request.getHeader("X-Session-ID"));
            }

            if (request.getHeader("X-Request-ID") != null) {
                out.put("o", request.getHeader("X-Request-ID"));
            }

            StringBuffer buf = new StringBuffer(512);
            buf.append(mapper.writeValueAsString(out));
            buf.append(StringUtil.__LINE_SEPARATOR);

            synchronized (this) {
                if (_writer == null) {
                    return;
                }

                _writer.write(buf.toString());
                _writer.flush();
            }
        } catch (IOException e) {
            Log.warn(e);
        }
    }

    @Override
    protected void doStart() throws Exception {
        if (_filename != null) {
            _fileOut = new RolloverFileOutputStream(_filename, _append,
                    _retainDays, TimeZone.getTimeZone("UTC"), null, null);
            _closeOut = true;
            Log.info("Opened " + getDatedFilename());
        } else {
            _fileOut = System.err;
        }

        _out = _fileOut;
        _writer = new OutputStreamWriter(_out);
        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        synchronized (this) {
            super.doStop();
            try {
                if (_writer != null) {
                    _writer.flush();
                }
            } catch (IOException e) {
                Log.ignore(e);
            }
            if (_out != null && _closeOut) {
                try {
                    _out.close();
                } catch (IOException e) {
                    Log.ignore(e);
                }
            }

            _out = null;
            _fileOut = null;
            _closeOut = false;
            _writer = null;
        }
    }
}
