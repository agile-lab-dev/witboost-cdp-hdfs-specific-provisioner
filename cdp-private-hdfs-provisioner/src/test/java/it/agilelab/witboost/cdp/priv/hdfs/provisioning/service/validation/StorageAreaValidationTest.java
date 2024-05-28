package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.validation;

import static org.junit.jupiter.api.Assertions.*;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.OutputPort;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.Specific;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.StorageArea;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.StorageSpecific;
import jakarta.validation.ConstraintViolationException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = {StorageAreaValidation.class, ValidationAutoConfiguration.class})
public class StorageAreaValidationTest {

    @Autowired
    StorageAreaValidation storageAreaValidation;

    @Test
    public void testValidateOk() {
        StorageSpecific storageSpecific = new StorageSpecific();
        storageSpecific.setRootFolder("/myprefix/healthcare/data-products/vaccinations/0");
        storageSpecific.setFolder("storage");
        StorageArea<StorageSpecific> storageArea = new StorageArea<>();
        storageArea.setId("my_id_storage");
        storageArea.setName("storage name");
        storageArea.setDescription("storage desc");
        storageArea.setKind("storage");
        storageArea.setSpecific(storageSpecific);

        var actualRes = storageAreaValidation.validate(storageArea);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateWrongType() {
        OutputPort<Specific> outputPort = new OutputPort<>();
        outputPort.setName("name");
        outputPort.setDescription("description");
        outputPort.setId("my_id_storage");
        outputPort.setKind("outputport");
        outputPort.setSpecific(new Specific());
        String expectedDesc = "The component my_id_storage is not of type StorageArea";

        var actualRes = storageAreaValidation.validate(outputPort);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateWrongSpecific() {
        Specific specific = new Specific();
        StorageArea<Specific> storageArea = new StorageArea<>();
        storageArea.setId("my_id_storage");
        storageArea.setName("storage name");
        storageArea.setDescription("storage desc");
        storageArea.setKind("storage");
        storageArea.setSpecific(specific);
        String expectedDesc = "The specific section of the component my_id_storage is not of type StorageSpecific";

        var actualRes = storageAreaValidation.validate(storageArea);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateInexistentPath() {
        StorageSpecific storageSpecific = new StorageSpecific();
        StorageArea<StorageSpecific> storageArea = new StorageArea<>();
        storageArea.setId("my_id_storage");
        storageArea.setName("storage name");
        storageArea.setDescription("storage desc");
        storageArea.setKind("storage");
        storageArea.setSpecific(storageSpecific);

        assertThrows(
                ConstraintViolationException.class,
                () -> storageAreaValidation.validate(storageArea),
                "validate.component.specific.folder: must not be blank, validate.component.specific.rootFolder: must not be blank");
    }

    @Test
    public void testValidateInvalidPath() {
        StorageSpecific storageSpecific = new StorageSpecific();
        storageSpecific.setRootFolder("/myprefix/healthcare/data-products/vaccinations\0");
        storageSpecific.setFolder("storage");
        StorageArea<StorageSpecific> storageArea = new StorageArea<>();
        storageArea.setId("my_id_storage");
        storageArea.setName("storage name");
        storageArea.setDescription("storage desc");
        storageArea.setKind("storage");
        storageArea.setSpecific(storageSpecific);
        String expectedDesc =
                "Failed to build path from specific storage. Root folder '/myprefix/healthcare/data-products/vaccinations\0' or folder 'storage' are invalid path strings";

        var actualRes = storageAreaValidation.validate(storageArea);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateNonRootPath() {
        StorageSpecific storageSpecific = new StorageSpecific();
        storageSpecific.setRootFolder("myprefix/healthcare/data-products/vaccinations");
        storageSpecific.setFolder("storage");
        StorageArea<StorageSpecific> storageArea = new StorageArea<>();
        storageArea.setId("my_id_storage");
        storageArea.setName("storage name");
        storageArea.setDescription("storage desc");
        storageArea.setKind("storage");
        storageArea.setSpecific(storageSpecific);
        String expectedDesc = "validate.component.specific.rootFolder: must match \"^/\"";

        assertThrows(
                ConstraintViolationException.class, () -> storageAreaValidation.validate(storageArea), expectedDesc);
    }
}
