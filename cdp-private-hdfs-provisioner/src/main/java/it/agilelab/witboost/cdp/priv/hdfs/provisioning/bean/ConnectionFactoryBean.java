package it.agilelab.witboost.cdp.priv.hdfs.provisioning.bean;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.LdapConfig;
import java.time.Duration;
import org.ldaptive.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class ConnectionFactoryBean {

    @Bean(initMethod = "initialize", destroyMethod = "close")
    public ConnectionFactory connectionFactory(LdapConfig ldapConfig) {
        return PooledConnectionFactory.builder()
                .config(ConnectionConfig.builder()
                        .url(ldapConfig.url())
                        .useStartTLS(ldapConfig.useTls())
                        .responseTimeout(Duration.ofMillis(ldapConfig.timeout()))
                        .connectionInitializers(BindConnectionInitializer.builder()
                                .dn(ldapConfig.bindUsername())
                                .credential(ldapConfig.bindPassword())
                                .build())
                        .build())
                .min(1)
                .max(5)
                .build();
    }

    @Bean
    public SearchOperation searchOperation(ConnectionFactory cf) {
        return new SearchOperation(cf);
    }
}
