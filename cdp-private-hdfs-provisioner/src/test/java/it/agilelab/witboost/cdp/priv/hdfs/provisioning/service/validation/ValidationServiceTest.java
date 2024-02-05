package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.DescriptorKind;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.util.ResourceUtils;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class ValidationServiceTest {

    private final ValidationServiceImpl validationService = new ValidationServiceImpl();

    @Test
    public void testValidateOutputPortOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptor_outputport_ok.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);

        var actualRes = validationService.validate(provisioningRequest);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateStorageOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptor_storage_ok.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);

        var actualRes = validationService.validate(provisioningRequest);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateWrongDescriptorKind() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptor_outputport_ok.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.DATAPRODUCT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "The descriptorKind field is not valid. Expected: 'COMPONENT_DESCRIPTOR', Actual: 'DATAPRODUCT_DESCRIPTOR'";

        var actualRes = validationService.validate(provisioningRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertEquals(expectedDesc, p.description());
                            assertTrue(p.cause().isEmpty());
                        });
    }

    @Test
    public void testValidateWrongDescriptorFormat() {
        String ymlDescriptor = "an_invalid_descriptor";
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc = "Failed to deserialize the Yaml Descriptor. Details: ";

        var actualRes = validationService.validate(provisioningRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertTrue(p.description().startsWith(expectedDesc));
                            assertTrue(p.cause().isPresent());
                        });
    }

    @Test
    public void testValidateMissingComponentIdToProvision() throws IOException {
        String ymlDescriptor =
                ResourceUtils.getContentFromResource(
                        "/descriptor_storage_missing_componentIdToProvision.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc = "Component with ID null not found in the Descriptor";

        var actualRes = validationService.validate(provisioningRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertEquals(expectedDesc, p.description());
                            assertTrue(p.cause().isEmpty());
                        });
    }

    @Test
    public void testValidateMissingComponentToProvision() throws IOException {
        String ymlDescriptor =
                ResourceUtils.getContentFromResource("/descriptor_storage_missing_component.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "Component with ID urn:dmb:cmp:healthcare:vaccinations:0:storage not found in the Descriptor";

        var actualRes = validationService.validate(provisioningRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertEquals(expectedDesc, p.description());
                            assertTrue(p.cause().isEmpty());
                        });
    }

    @Test
    public void testValidateMissingComponentKindToProvision() throws IOException {
        String ymlDescriptor =
                ResourceUtils.getContentFromResource("/descriptor_storage_missing_componentKind.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "Component Kind not found for the component with ID urn:dmb:cmp:healthcare:vaccinations:0:storage";

        var actualRes = validationService.validate(provisioningRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertEquals(expectedDesc, p.description());
                            assertTrue(p.cause().isEmpty());
                        });
    }

    @Test
    public void testValidateWrongComponentKindToProvision() throws IOException {
        String ymlDescriptor =
                ResourceUtils.getContentFromResource("/descriptor_storage_wrong_componentKind.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "The kind 'wrong' of the component to provision is not supported by this Specific Provisioner";

        var actualRes = validationService.validate(provisioningRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes
                .getLeft()
                .problems()
                .forEach(
                        p -> {
                            assertEquals(expectedDesc, p.description());
                            assertTrue(p.cause().isEmpty());
                        });
    }
}
