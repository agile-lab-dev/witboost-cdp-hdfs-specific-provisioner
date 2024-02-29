package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.*;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.*;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.validation.ValidationService;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProvisionServiceTest {
    @Mock
    private ValidationService validationService;

    @Mock
    private StorageAreaHandler storageAreaHandler;

    @Mock
    private OutputPortHandler outputPortHandler;

    @InjectMocks
    private ProvisionServiceImpl provisionService;

    @Test
    public void testValidateOk() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        when(validationService.validate(provisioningRequest))
                .thenReturn(right(new ProvisionRequest<Specific>(null, null, false)));
        var expectedRes = new ValidationResult(true);

        var actualRes = provisionService.validate(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testValidateError() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("error")));
        when(validationService.validate(provisioningRequest)).thenReturn(left(failedOperation));
        var expectedRes = new ValidationResult(false).error(new ValidationError(List.of("error")));

        var actualRes = provisionService.validate(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testProvisionValidationError() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("error")));
        when(validationService.validate(provisioningRequest)).thenReturn(left(failedOperation));

        var ex = assertThrows(
                SpecificProvisionerValidationException.class, () -> provisionService.provision(provisioningRequest));
        assertEquals(failedOperation, ex.getFailedOperation());
    }

    @Test
    public void testProvisionUnsupportedKind() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        StorageArea<Specific> storageArea = new StorageArea<>();
        storageArea.setKind("unsupported");
        when(validationService.validate(provisioningRequest))
                .thenReturn(right(new ProvisionRequest<>(null, storageArea, false)));
        String expectedDesc = "The kind 'unsupported' of the component is not supported by this Specific Provisioner";
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem(expectedDesc)));

        var ex = assertThrows(
                SpecificProvisionerValidationException.class, () -> provisionService.provision(provisioningRequest));
        assertEquals(failedOperation, ex.getFailedOperation());
    }

    @Test
    public void testProvisionStorageAreaOk() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        StorageArea<Specific> storageArea = new StorageArea<>();
        storageArea.setKind("storage");
        var provisionRequest = new ProvisionRequest<>(null, storageArea, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        String createdFolderPath = "folder";
        when(storageAreaHandler.create(provisionRequest)).thenReturn(right(createdFolderPath));
        var privateInfo = Map.of("path", createdFolderPath);
        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "")
                .info(new Info(JsonNodeFactory.instance.objectNode(), privateInfo));

        var actualRes = provisionService.provision(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testProvisionStorageAreaFailHandler() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        StorageArea<Specific> storageArea = new StorageArea<>();
        storageArea.setKind("storage");
        var provisionRequest = new ProvisionRequest<>(null, storageArea, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        String expectedDesc = "Error on Ranger";
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem(expectedDesc)));
        when(storageAreaHandler.create(provisionRequest)).thenReturn(left(failedOperation));

        var ex = assertThrows(
                SpecificProvisionerValidationException.class, () -> provisionService.provision(provisioningRequest));
        assertEquals(failedOperation, ex.getFailedOperation());
    }

    @Test
    public void testProvisionOutputPortOk() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setKind("outputport");
        var provisionRequest = new ProvisionRequest<>(null, outputPort, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        String folderPath = "folder";
        when(outputPortHandler.create(provisionRequest)).thenReturn(right(folderPath));
        var publicInfo = Map.of("path", Map.of("type", "string", "label", "HDFS Path", "value", folderPath));
        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "")
                .info(new Info(publicInfo, JsonNodeFactory.instance.objectNode()));

        var actualRes = provisionService.provision(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testProvisionOutputPortFailHandler() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setKind("outputport");
        var provisionRequest = new ProvisionRequest<>(null, outputPort, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        String expectedDesc = "Error on Ranger";
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem(expectedDesc)));
        when(outputPortHandler.create(provisionRequest)).thenReturn(left(failedOperation));

        var ex = assertThrows(
                SpecificProvisionerValidationException.class, () -> provisionService.provision(provisioningRequest));
        assertEquals(failedOperation, ex.getFailedOperation());
    }

    @Test
    public void testUnprovisionValidationError() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("error")));
        when(validationService.validate(provisioningRequest)).thenReturn(left(failedOperation));

        var ex = assertThrows(
                SpecificProvisionerValidationException.class, () -> provisionService.unprovision(provisioningRequest));
        assertEquals(failedOperation, ex.getFailedOperation());
    }

    @Test
    public void testUnprovisionUnsupportedKind() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        StorageArea<Specific> storageArea = new StorageArea<>();
        storageArea.setKind("unsupported");
        when(validationService.validate(provisioningRequest))
                .thenReturn(right(new ProvisionRequest<>(null, storageArea, false)));
        String expectedDesc = "The kind 'unsupported' of the component is not supported by this Specific Provisioner";
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem(expectedDesc)));

        var ex = assertThrows(
                SpecificProvisionerValidationException.class, () -> provisionService.unprovision(provisioningRequest));
        assertEquals(failedOperation, ex.getFailedOperation());
    }

    @Test
    public void testUnprovisionStorageAreaOk() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        StorageArea<Specific> storageArea = new StorageArea<>();
        storageArea.setKind("storage");
        var provisionRequest = new ProvisionRequest<>(null, storageArea, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(storageAreaHandler.destroy(provisionRequest)).thenReturn(right(null));
        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "");

        var actualRes = provisionService.unprovision(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testUnprovisionStorageAreaFailHandler() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        StorageArea<Specific> storageArea = new StorageArea<>();
        storageArea.setKind("storage");
        var provisionRequest = new ProvisionRequest<>(null, storageArea, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        String expectedDesc = "Error on Ranger";
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem(expectedDesc)));
        when(storageAreaHandler.destroy(provisionRequest)).thenReturn(left(failedOperation));

        var ex = assertThrows(
                SpecificProvisionerValidationException.class, () -> provisionService.unprovision(provisioningRequest));
        assertEquals(failedOperation, ex.getFailedOperation());
    }

    @Test
    public void testUnprovisionOutputPortOk() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setKind("outputport");
        var provisionRequest = new ProvisionRequest<>(null, outputPort, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(outputPortHandler.destroy(provisionRequest)).thenReturn(right(null));
        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "");

        var actualRes = provisionService.unprovision(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testUnprovisionOutputPortFailHandler() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setKind("outputport");
        var provisionRequest = new ProvisionRequest<>(null, outputPort, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        String expectedDesc = "Error on Ranger";
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem(expectedDesc)));
        when(outputPortHandler.destroy(provisionRequest)).thenReturn(left(failedOperation));

        var ex = assertThrows(
                SpecificProvisionerValidationException.class, () -> provisionService.unprovision(provisioningRequest));
        assertEquals(failedOperation, ex.getFailedOperation());
    }

    @Test
    public void testUpdateAclOutputPortOk() {
        // Given
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        provisioningRequest.setDescriptorKind(DescriptorKind.COMPONENT_DESCRIPTOR);
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setKind("outputport");
        List<String> refs = List.of();
        UpdateAclRequest updateAclRequest = new UpdateAclRequest(refs, new ProvisionInfo());
        ProvisionRequest<Specific> provisionRequest = new ProvisionRequest<>(null, outputPort, false);

        ProvisioningStatus provisioningStatus = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "");
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(outputPortHandler.updateAcl(refs, provisionRequest)).thenReturn(right(provisioningStatus));

        // When
        ProvisioningStatus actualRes = provisionService.updateAcl(updateAclRequest);

        // Then
        ProvisioningStatus expectedRes = provisioningStatus;
        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testUpdateAclOutputPortFailing() {
        // Given
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        provisioningRequest.setDescriptorKind(DescriptorKind.COMPONENT_DESCRIPTOR);
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setKind("outputport");
        List<String> refs = List.of();
        UpdateAclRequest updateAclRequest = new UpdateAclRequest(refs, new ProvisionInfo());
        ProvisionRequest<Specific> provisionRequest = new ProvisionRequest<>(null, outputPort, false);

        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(outputPortHandler.updateAcl(refs, provisionRequest))
                .thenReturn(left(new FailedOperation(List.of(new Problem("")))));

        // When and Then
        assertThatThrownBy(() -> provisionService.updateAcl(updateAclRequest))
                .isInstanceOf(SpecificProvisionerValidationException.class)
                .hasFieldOrPropertyWithValue("failedOperation", new FailedOperation(List.of(new Problem(""))));
    }

    @Test
    public void testUpdateAclStorageArea() {
        // Given
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        provisioningRequest.setDescriptorKind(DescriptorKind.COMPONENT_DESCRIPTOR);
        StorageArea<Specific> storageArea = new StorageArea<>();
        storageArea.setKind("storage");
        List<String> refs = List.of();
        UpdateAclRequest updateAclRequest = new UpdateAclRequest(refs, new ProvisionInfo());
        ProvisionRequest<Specific> provisionRequest = new ProvisionRequest<>(null, storageArea, false);

        FailedOperation failedOperation = new FailedOperation(List.of(new Problem("")));

        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(storageAreaHandler.updateAcl()).thenReturn(failedOperation);

        // When and Then
        assertThatThrownBy(() -> provisionService.updateAcl(updateAclRequest))
                .isInstanceOf(SpecificProvisionerValidationException.class)
                .hasFieldOrPropertyWithValue("failedOperation", failedOperation);
    }

    @Test
    public void testUpdateAclFailingBecauseOfUnknownComponentKind() {
        // Given
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        provisioningRequest.setDescriptorKind(DescriptorKind.COMPONENT_DESCRIPTOR);
        StorageArea<Specific> storageArea = new StorageArea<>();
        storageArea.setKind("unknown");
        List<String> refs = List.of();
        UpdateAclRequest updateAclRequest = new UpdateAclRequest(refs, new ProvisionInfo());
        ProvisionRequest<Specific> provisionRequest = new ProvisionRequest<>(null, storageArea, false);

        FailedOperation failedOperation = new FailedOperation(List.of(
                new Problem("The kind 'unknown' of the component is not supported by this Specific Provisioner")));

        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        // When and Then
        assertThatThrownBy(() -> provisionService.updateAcl(updateAclRequest))
                .isInstanceOf(SpecificProvisionerValidationException.class)
                .hasFieldOrPropertyWithValue("failedOperation", failedOperation);
    }

    @Test
    public void testUpdateAclFailingBecauseOfFailingValidation() {
        // Given
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        provisioningRequest.setDescriptorKind(DescriptorKind.COMPONENT_DESCRIPTOR);
        List<String> refs = List.of();
        UpdateAclRequest updateAclRequest = new UpdateAclRequest(refs, new ProvisionInfo());

        FailedOperation failedOperation =
                new FailedOperation(List.of(new Problem("Something went wrong with the validation of ProvisionInfo")));

        when(validationService.validate(provisioningRequest)).thenReturn(left(failedOperation));

        // When and Then
        assertThatThrownBy(() -> provisionService.updateAcl(updateAclRequest))
                .isInstanceOf(SpecificProvisionerValidationException.class)
                .hasFieldOrPropertyWithValue("failedOperation", failedOperation);
    }
}
