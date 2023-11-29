package cn.hashdata.dlagent.api.error;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;

public class DlRuntimeException extends RuntimeException {

    @Getter
    private final String hint;

    public DlRuntimeException() {
        this(null, null, null);
    }

    public DlRuntimeException(String message) {
        this(message, null, null);
    }

    public DlRuntimeException(String message, String hint) {
        this(message, hint, null);
    }

    public DlRuntimeException(Throwable cause) {
        this(StringUtils.defaultIfBlank(cause.getMessage(), cause.getClass().getName()), cause);
    }

    public DlRuntimeException(String message, Throwable cause) {
        this(message, null, cause);
    }

    public DlRuntimeException(String message, String hint, Throwable cause) {
        super(message, cause);
        this.hint = hint;
    }

}
