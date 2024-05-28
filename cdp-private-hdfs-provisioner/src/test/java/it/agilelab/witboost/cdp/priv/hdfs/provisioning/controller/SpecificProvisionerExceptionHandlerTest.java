package it.agilelab.witboost.cdp.priv.hdfs.provisioning.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.RequestValidationError;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.SystemError;
import jakarta.validation.ConstraintViolationException;
import java.util.Collections;
import java.util.Set;
import org.hibernate.validator.internal.engine.ConstraintViolationImpl;
import org.hibernate.validator.internal.engine.path.PathImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;

@ExtendWith(MockitoExtension.class)
@WebMvcTest(SpecificProvisionerExceptionHandler.class)
public class SpecificProvisionerExceptionHandlerTest {

    @InjectMocks
    SpecificProvisionerExceptionHandler specificProvisionerExceptionHandler;

    @Test
    void testHandleConflictSystemError() {
        RuntimeException runtimeException = new RuntimeException();
        String expectedError =
                "An unexpected error occurred while processing the request. Please try again later. If the issue still persists, contact the platform team for assistance! Details: ";

        SystemError error = specificProvisionerExceptionHandler.handleSystemError(runtimeException);

        assertTrue(error.getError().startsWith(expectedError));
    }

    @Test
    void testHandleConflictRequestValidationError() {
        String expectedError = "Validation error";
        SpecificProvisionerValidationException specificProvisionerValidationException =
                new SpecificProvisionerValidationException(
                        new FailedOperation(Collections.singletonList(new Problem(expectedError))));

        RequestValidationError requestValidationError =
                specificProvisionerExceptionHandler.handleValidationException(specificProvisionerValidationException);

        assertEquals(1, requestValidationError.getErrors().size());
        assertEquals(
                requestValidationError.getUserMessage(),
                "Validation on the received descriptor failed, check the details for more information");
        requestValidationError.getErrors().forEach(e -> assertEquals(expectedError, e));
    }

    @Test
    void testHandleConflictConstraintValidationError() {
        String expectedError = "Validation error";
        String expectedValidationMessage = "validation.a.path is wrong";
        ConstraintViolationException exception = new ConstraintViolationException(
                expectedError,
                Set.of(ConstraintViolationImpl.forBeanValidation(
                        null,
                        null,
                        null,
                        "is wrong",
                        null,
                        null,
                        null,
                        null,
                        PathImpl.createPathFromString("validation.a.path"),
                        null,
                        null)));

        RequestValidationError requestValidationError =
                specificProvisionerExceptionHandler.handleValidationException(exception);

        assertEquals(1, requestValidationError.getErrors().size());
        assertEquals(
                requestValidationError.getUserMessage(),
                "Validation on the received descriptor failed, check the details for more information");
        assertEquals(requestValidationError.getInputErrorField(), "validation.a.path");
        requestValidationError.getMoreInfo().getProblems().forEach(e -> assertEquals(expectedValidationMessage, e));
        requestValidationError.getErrors().forEach(e -> assertEquals(expectedValidationMessage, e));
    }
}
