package it.agilelab.witboost.cdp.priv.hdfs.provisioning.rest;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.KerberosConfig;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.security.kerberos.client.KerberosRestTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KerberosRestBean {

    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public RestTemplate kerberosRestTemplate(KerberosConfig kConfig) {
        return new KerberosRestTemplate(kConfig.keytabLocation(), kConfig.principal());
    }
}
