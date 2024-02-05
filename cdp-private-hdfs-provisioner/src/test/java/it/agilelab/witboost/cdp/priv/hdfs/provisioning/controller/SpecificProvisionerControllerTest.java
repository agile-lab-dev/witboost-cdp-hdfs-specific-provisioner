package it.agilelab.witboost.cdp.priv.hdfs.provisioning.controller;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.DescriptorKind;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ValidationError;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ValidationResult;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision.ProvisionService;
import java.util.Collections;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@ExtendWith(MockitoExtension.class)
public class SpecificProvisionerControllerTest {

    @Mock private ProvisionService provisionService;
    @InjectMocks private SpecificProvisionerController specificProvisionerController;

    @Test
    void testValidateOk() throws Exception {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(mockHttpServletRequest));
        when(provisionService.validate(provisioningRequest)).thenReturn(new ValidationResult(true));

        ResponseEntity<ValidationResult> actualRes =
                specificProvisionerController.validate(provisioningRequest);

        assertEquals(HttpStatusCode.valueOf(200), actualRes.getStatusCode());
        assertTrue(Objects.requireNonNull(actualRes.getBody()).getValid());
    }

    @Test
    void testValidateHasError() throws Exception {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(mockHttpServletRequest));
        String expectedError = "Validation error";
        when(provisionService.validate(provisioningRequest))
                .thenReturn(
                        new ValidationResult(false)
                                .error(new ValidationError(Collections.singletonList(expectedError))));

        ResponseEntity<ValidationResult> actualRes =
                specificProvisionerController.validate(provisioningRequest);

        assertEquals(HttpStatusCode.valueOf(200), actualRes.getStatusCode());
        assertFalse(Objects.requireNonNull(actualRes.getBody()).getValid());
        assertEquals(1, actualRes.getBody().getError().getErrors().size());
        actualRes.getBody().getError().getErrors().forEach(p -> assertEquals(expectedError, p));
    }
}
