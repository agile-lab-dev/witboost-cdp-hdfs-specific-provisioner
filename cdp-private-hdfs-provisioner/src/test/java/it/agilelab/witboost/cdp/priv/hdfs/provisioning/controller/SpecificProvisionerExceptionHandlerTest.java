package it.agilelab.witboost.cdp.priv.hdfs.provisioning.controller;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.RequestValidationError;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.SystemError;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;

@ExtendWith(MockitoExtension.class)
@WebMvcTest(SpecificProvisionerExceptionHandler.class)
public class SpecificProvisionerExceptionHandlerTest {

    @InjectMocks SpecificProvisionerExceptionHandler specificProvisionerExceptionHandler;

    @Test
    void testHandleConflictSystemError() {

        RuntimeException runtimeException = new RuntimeException();

        SystemError error = specificProvisionerExceptionHandler.handleConflict(runtimeException);

        Assertions.assertNull(error.getError());
    }

    @Test
    void testHandleConflictRequestValidationError() {

        SpecificProvisionerValidationException specificProvisionerValidationException =
                new SpecificProvisionerValidationException("");

        RequestValidationError requestValidationError =
                specificProvisionerExceptionHandler.handleConflict(specificProvisionerValidationException);

        Assertions.assertEquals(requestValidationError.getErrors(), List.of(""));
    }
}
