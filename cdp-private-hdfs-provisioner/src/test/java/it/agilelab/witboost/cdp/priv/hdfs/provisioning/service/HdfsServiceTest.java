package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import io.vavr.control.Either;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.HdfsConfig;
import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.web.client.AutoConfigureWebClient;
import org.springframework.boot.test.autoconfigure.web.client.RestClientTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClientResponseException;

@RestClientTest(HdfsService.class)
@AutoConfigureWebClient(registerRestTemplate = true)
@EnableConfigurationProperties({
    HdfsConfig.class,
})
public class HdfsServiceTest {

    @Autowired
    private MockRestServiceServer server;

    @Autowired
    private HdfsService hdfsService;

    @BeforeEach
    void setUp() {
        server.reset();
    }

    @AfterEach
    void tearDown() {
        server.verify();
    }

    @Test
    public void testCreateFolderShouldReturnOk() {
        String path = "/my/folder";
        String res = """
            {"boolean": true}
             """;
        server.expect(requestTo("http://hdfs-host/webhdfs/v1/my/folder?op=MKDIRS"))
                .andRespond(withSuccess(res, MediaType.APPLICATION_JSON));

        Either<FailedOperation, String> actualRes = hdfsService.createFolder(path);

        assertTrue(actualRes.isRight());
        assertEquals(path, actualRes.get());
    }

    @Test
    public void testCreateFolderShouldFail() {
        String path = "/my/folder";
        String res = """
            {"boolean": false}
             """;
        server.expect(requestTo("http://hdfs-host/webhdfs/v1/my/folder?op=MKDIRS"))
                .andRespond(withSuccess(res, MediaType.APPLICATION_JSON));
        var expectedRes = new FailedOperation(
                Collections.singletonList(
                        new Problem(
                                "Failed to create the folder '/my/folder'. Please try again and if the issue persists contact the platform team")));

        Either<FailedOperation, String> actualRes = hdfsService.createFolder(path);

        assertTrue(actualRes.isLeft());
        assertEquals(expectedRes, actualRes.getLeft());
    }

    @Test
    public void testCreateFolderShouldReturnBadStatusCode() {
        String path = "/my/folder";
        server.expect(requestTo("http://hdfs-host/webhdfs/v1/my/folder?op=MKDIRS"))
                .andRespond(withServerError());
        var expectedDesc =
                "Failed to create the folder '/my/folder'. Please try again and if the issue persists contact the platform team. Details: ";

        Either<FailedOperation, String> actualRes = hdfsService.createFolder(path);

        assertTrue(actualRes.isLeft());
        actualRes.getLeft().problems().forEach(p -> {
            assertTrue(p.description().startsWith(expectedDesc));
            assertTrue(p.cause().isPresent());
            assertInstanceOf(RestClientResponseException.class, p.cause().get());
        });
    }

    @Test
    public void testDeleteFolderShouldReturnOk() {
        String path = "/my/folder";
        String res = """
            {"boolean": true}
             """;
        server.expect(requestTo("http://hdfs-host/webhdfs/v1/my/folder?op=DELETE&recursive=true"))
                .andRespond(withSuccess(res, MediaType.APPLICATION_JSON));

        Either<FailedOperation, String> actualRes = hdfsService.deleteFolder(path);

        assertTrue(actualRes.isRight());
        assertEquals(path, actualRes.get());
    }

    @Test
    public void testDeleteFolderAlreadyDeleted() {
        String path = "/my/folder";
        String res = """
            {"boolean": false}
             """;
        server.expect(requestTo("http://hdfs-host/webhdfs/v1/my/folder?op=DELETE&recursive=true"))
                .andRespond(withSuccess(res, MediaType.APPLICATION_JSON));

        Either<FailedOperation, String> actualRes = hdfsService.deleteFolder(path);

        assertTrue(actualRes.isRight());
        assertEquals(path, actualRes.get());
    }

    @Test
    public void testDeleteFolderShouldReturnBadStatusCode() {
        String path = "/my/folder";
        server.expect(requestTo("http://hdfs-host/webhdfs/v1/my/folder?op=DELETE&recursive=true"))
                .andRespond(withServerError());
        var expectedDesc =
                "Failed to delete the folder '/my/folder'. Please try again and if the issue persists contact the platform team. Details: ";

        Either<FailedOperation, String> actualRes = hdfsService.deleteFolder(path);

        assertTrue(actualRes.isLeft());
        actualRes.getLeft().problems().forEach(p -> {
            assertTrue(p.description().startsWith(expectedDesc));
            assertTrue(p.cause().isPresent());
            assertInstanceOf(RestClientResponseException.class, p.cause().get());
        });
    }
}
