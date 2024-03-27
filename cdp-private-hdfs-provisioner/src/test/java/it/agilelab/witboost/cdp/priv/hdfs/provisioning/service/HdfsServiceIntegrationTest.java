package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.HdfsConfig;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.client.RestTemplateAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.support.HttpRequestWrapper;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@SpringBootTest(classes = {RestTemplateAutoConfiguration.class})
@Testcontainers
public class HdfsServiceIntegrationTest {

    @Container
    static GenericContainer<?> miniclusterContainer = new GenericContainer<>(
                    new ImageFromDockerfile("minidfscluster", false)
                            .withFileFromClasspath("Dockerfile", "minicluster/Dockerfile"))
            .withExposedPorts(9001)
            .waitingFor(Wait.forHttp("/jmx?qry=Hadoop:service%3DNameNode,name%3DNameNodeStatus")
                    .forResponsePredicate(s -> s.contains("active")))
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("minicluster/hdfs-site.xml"),
                    "/work/hadoop-3.3.6/etc/hadoop/hdfs-site.xml");

    @Autowired
    private RestTemplateBuilder restTemplateBuilder;

    @PostConstruct
    public void addUserNameQueryParam() {
        this.restTemplateBuilder = restTemplateBuilder.additionalInterceptors(new ClientHttpRequestInterceptor() {
            @Override
            public @NotNull ClientHttpResponse intercept(
                    @NotNull HttpRequest request, byte @NotNull [] body, @NotNull ClientHttpRequestExecution execution)
                    throws IOException {
                URI newUri = appendUri(request.getURI().toString(), "user.name=root");
                var wrappedRequest = new HttpRequestWrapper(request) {
                    @Override
                    public @NotNull URI getURI() {
                        return newUri;
                    }
                };
                return execution.execute(wrappedRequest, body);
            }
        });
    }

    @Test
    void testMiniclusterIsRunning() {
        assertThat(miniclusterContainer.isRunning()).isTrue();
    }

    @Test
    void testIntegrationHdfsServiceCreateAndDeleteIsWorking() {

        String url =
                String.format("http://%s:%s", miniclusterContainer.getHost(), miniclusterContainer.getMappedPort(9001));
        HdfsConfig integrationHdfsConfig = new HdfsConfig(url, url, 10);
        HdfsService hdfsService = new HdfsServiceImpl(restTemplateBuilder.build(), integrationHdfsConfig);

        var resCreation = hdfsService.createFolder("/myfolder");

        assertThat(resCreation.isRight()).isTrue();
        assertEquals("/myfolder", resCreation.get());

        var resDeletion = hdfsService.deleteFolder("/myfolder");

        assertThat(resDeletion.isRight()).isTrue();
        assertEquals("/myfolder", resDeletion.get());
    }

    @Test
    void testIntegrationHdfsServiceDeleteAlreadyDeletedIsWorking() {
        String url =
                String.format("http://%s:%s", miniclusterContainer.getHost(), miniclusterContainer.getMappedPort(9001));
        HdfsConfig integrationHdfsConfig = new HdfsConfig(url, url, 10);
        HdfsService hdfsService = new HdfsServiceImpl(restTemplateBuilder.build(), integrationHdfsConfig);

        var resDeletion = hdfsService.deleteFolder("/notexisting");

        assertThat(resDeletion.isRight()).isTrue();
        assertEquals("/notexisting", resDeletion.get());
    }

    private URI appendUri(String uri, String appendQuery) {
        try {
            URI oldUri = new URI(uri);
            return new URI(
                    oldUri.getScheme(),
                    oldUri.getAuthority(),
                    oldUri.getPath(),
                    oldUri.getQuery() == null ? appendQuery : oldUri.getQuery() + "&" + appendQuery,
                    oldUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
