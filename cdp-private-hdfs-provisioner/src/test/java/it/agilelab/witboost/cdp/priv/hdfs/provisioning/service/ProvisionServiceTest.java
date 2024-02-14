package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.DataProduct;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.ProvisionRequest;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.StorageArea;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.DescriptorKind;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision.ProvisionServiceImpl;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.validation.ValidationService;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProvisionServiceTest {

    @Mock
    private ValidationService validationService;

    @InjectMocks
    private ProvisionServiceImpl provisionService;

    @Test
    public void validateOk() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        when(validationService.validate(provisioningRequest))
                .thenReturn(right(new ProvisionRequest<>(new DataProduct(), new StorageArea<>(), false)));

        var actualRes = provisionService.validate(provisioningRequest);

        assertTrue(actualRes.getValid());
    }

    @Test
    public void validateHasError() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        String expectedError = "Validation error";
        when(validationService.validate(provisioningRequest))
                .thenReturn(left(new FailedOperation(Collections.singletonList(new Problem(expectedError)))));

        var actualRes = provisionService.validate(provisioningRequest);

        assertFalse(actualRes.getValid());
        assertEquals(1, actualRes.getError().getErrors().size());
        actualRes.getError().getErrors().forEach(p -> assertEquals(expectedError, p));
    }
}
