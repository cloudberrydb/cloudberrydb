package cn.hashdata.dlagent.api.utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
public class CharsetUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CharsetUtils.class);

    private static final Map<String, String[]> KNOWN_ENCODINGS = Collections.unmodifiableMap(
            new HashMap<String, String[]>() {{
                //Note: this list should match the set of supported server
                // encodings found in backend/util/mb/encnames.c
                put("SQL_ASCII", new String[]{"ASCII", "US-ASCII"});
                put("UNICODE", new String[]{"UTF-8", "UTF8"});
                put("UTF8", new String[]{"UTF-8", "UTF8"});
                put("LATIN1", new String[]{"ISO8859_1"});
                put("LATIN2", new String[]{"ISO8859_2"});
                put("LATIN3", new String[]{"ISO8859_3"});
                put("LATIN4", new String[]{"ISO8859_4"});
                put("ISO_8859_5", new String[]{"ISO8859_5"});
                put("ISO_8859_6", new String[]{"ISO8859_6"});
                put("ISO_8859_7", new String[]{"ISO8859_7"});
                put("ISO_8859_8", new String[]{"ISO8859_8"});
                put("LATIN5", new String[]{"ISO8859_9"});
                put("LATIN7", new String[]{"ISO8859_13"});
                put("LATIN9", new String[]{"ISO8859_15_FDIS"});
                put("EUC_JP", new String[]{"EUC_JP"});
                put("EUC_CN", new String[]{"EUC_CN"});
                put("EUC_KR", new String[]{"EUC_KR"});
                put("JOHAB", new String[]{"Johab"});
                put("EUC_TW", new String[]{"EUC_TW"});
                put("SJIS", new String[]{"MS932", "SJIS"});
                put("BIG5", new String[]{"Big5", "MS950", "Cp950"});
                put("GBK", new String[]{"GBK", "MS936"});
                put("UHC", new String[]{"MS949", "Cp949", "Cp949C"});
                put("TCVN", new String[]{"Cp1258"});
                put("WIN1250", new String[]{"Cp1250"});
                put("WIN1251", new String[]{"Windows-1251", "Cp1251"});
                put("WIN1252", new String[]{"Windows-1252", "Cp1252"});
                put("WIN1253", new String[]{"Windows-1253", "Cp1253"});
                put("WIN1254", new String[]{"Windows-1254", "Cp1254"});
                put("WIN1255", new String[]{"Windows-1255", "Cp1255"});
                put("WIN1256", new String[]{"Cp1256"});
                put("WIN1257", new String[]{"Windows-1257", "Cp1257"});
                put("WIN1258", new String[]{"Windows-1258", "Cp1258"});
                put("WIN866", new String[]{"Cp866"});
                put("WIN874", new String[]{"MS874", "Cp874"});
                put("WIN", new String[]{"Cp1251"});
                put("ALT", new String[]{"Cp866"});
                // We prefer KOI8-U, since it is a superset of KOI8-R.
                put("KOI8", new String[]{"KOI8_U", "KOI8_R"});
                put("KOI8R", new String[]{"KOI8_R"});
                put("KOI8U", new String[]{"KOI8_U"});
                // The following encodings do not have a java equivalent
                // Adding them below for documentation completeness
                // put("MULE_INTERNAL", new String[0]);
                // put("LATIN6", new String[0]);
                // put("LATIN8", new String[0]);
                // put("LATIN10", new String[0]);
                // put("GB18030", new String[0]);
                // put("SHIFT_JIS_2004", new String[0]);
                // put("EUC_JIS_2004", new String[0]);
            }}
    );

    /**
     * Return the java {@link Charset} for a given database encoding
     * {@code name}. Greenplum supports a list of well-known encodings that
     * we map to a java charset
     *
     * @param name the database encoding name
     * @return the {@link Charset} for the given name
     */
    public Charset forName(String name) {
        if ("UTF8".equals(name)) {
            return StandardCharsets.UTF_8;
        }
        // Search for an encoding from a list of known encodings, then find
        // a suitable encoding in the JVM.
        String[] encodings = KNOWN_ENCODINGS.get(name);
        if (encodings != null) {
            for (String encoding : encodings) {
                LOG.trace("Searching for encoding {}", encoding);
                if (Charset.isSupported(encoding)) {
                    return Charset.forName(encoding);
                }
            }
        }

        LOG.debug("{} encoding not found", name);
        throw new IllegalCharsetNameException(String.format("The '%s' encoding is not supported", name));
    }
}