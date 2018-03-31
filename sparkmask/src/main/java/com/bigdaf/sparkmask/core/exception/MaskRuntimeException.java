package com.bigdaf.sparkmask.core.exception;

public class MaskRuntimeException extends RuntimeException{
    public MaskRuntimeException() {
        super();
    }

    public MaskRuntimeException(String message) {
        super(message);
    }

    public MaskRuntimeException(Exception e) {
        super(e);
    }

    public MaskRuntimeException(String message, Throwable e) {
        super(message, e);
    }
}
