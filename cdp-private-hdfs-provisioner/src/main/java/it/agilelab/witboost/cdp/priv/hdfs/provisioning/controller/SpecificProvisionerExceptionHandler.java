package it.agilelab.witboost.cdp.priv.hdfs.provisioning.controller;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.RequestValidationError;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.SystemError;
import java.util.ArrayList;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Exception handler for the API layer.
 *
 * <p>The following methods wrap generic exceptions into 400 and 500 errors. Implement your own
 * exception handlers based on the business exception that the provisioner throws. No further
 * modifications need to be done outside this file to make it work, as Spring identifies at startup
 * the handlers with the @ExceptionHandler annotation
 */
@RestControllerAdvice
public class SpecificProvisionerExceptionHandler {

    @ExceptionHandler({SpecificProvisionerValidationException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    protected RequestValidationError handleConflict(SpecificProvisionerValidationException ex) {
        List<String> list = new ArrayList<>();
        list.add(ex.getMessage());
        return new RequestValidationError(list);
    }

    @ExceptionHandler({RuntimeException.class})
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    protected SystemError handleConflict(RuntimeException ex) {
        return new SystemError(ex.getMessage());
    }
}
