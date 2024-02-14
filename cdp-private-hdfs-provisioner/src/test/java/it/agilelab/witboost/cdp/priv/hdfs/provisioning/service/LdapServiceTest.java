package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.LdapConfig;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.CDPGroup;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.CDPUser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.ldaptive.*;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LdapServiceTest {

    @Mock
    private SearchOperation searchOperation;

    @Mock
    private LdapConfig ldapConfig;

    @InjectMocks
    private LdapServiceImpl ldapService;

    private final String mail = "user1@email.com";
    private final String cnGroup = "group1";

    @Test
    public void testFindUserByMailWithExistingUser() throws LdapException {
        String cnUser = "user1";
        var searchResponse = SearchResponse.builder()
                .entry(LdapEntry.builder()
                        .dn("")
                        .attributes(
                                LdapAttribute.builder()
                                        .name("cn")
                                        .values(cnUser)
                                        .build(),
                                LdapAttribute.builder()
                                        .name("mail")
                                        .values(mail)
                                        .build())
                        .build())
                .build();
        when(ldapConfig.userSearchFilter()).thenReturn("(mail={mail})");
        when(ldapConfig.userAttributeName()).thenReturn("cn");
        when(searchOperation.execute(any(SearchRequest.class))).thenReturn(searchResponse);
        CDPUser expectedUser = new CDPUser(cnUser, mail);

        var actualRes = ldapService.findUserByMail(mail);

        assertTrue(actualRes.isRight());
        assertTrue(actualRes.get().isDefined());
        assertEquals(expectedUser, actualRes.get().get());
    }

    @Test
    public void testFindUserByMailWithNotExistingUser() throws LdapException {
        var searchResponse = SearchResponse.builder().build();
        when(ldapConfig.userSearchFilter()).thenReturn("(mail={mail})");
        when(searchOperation.execute(any(SearchRequest.class))).thenReturn(searchResponse);

        var actualRes = ldapService.findUserByMail(mail);

        assertTrue(actualRes.isRight());
        assertTrue(actualRes.get().isEmpty());
    }

    @Test
    public void testFindUserByMailReturnError() throws LdapException {
        LdapException ex = new LdapException(ResultCode.TIME_LIMIT_EXCEEDED, "");
        when(ldapConfig.userSearchFilter()).thenReturn("(mail={mail})");
        when(searchOperation.execute(any(SearchRequest.class))).thenThrow(ex);
        String expectedDesc =
                "An error occurred while searching for the user 'user1@email.com' on LDAP. Please try again and if the error persists contact the platform team. Details: ";

        var actualRes = ldapService.findUserByMail(mail);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertTrue(p.description().startsWith(expectedDesc));
            assertTrue(p.cause().isPresent());
            assertEquals(ex, p.cause().get());
        });
    }

    @Test
    public void testFindGroupByNameWithExistingGroup() throws LdapException {
        var searchResponse = SearchResponse.builder()
                .entry(LdapEntry.builder()
                        .dn("")
                        .attributes(LdapAttribute.builder()
                                .name("cn")
                                .values(cnGroup)
                                .build())
                        .build())
                .build();
        when(ldapConfig.groupSearchFilter()).thenReturn("(&(objectClass=groupOfNames)(cn={group}))");
        when(ldapConfig.groupAttributeName()).thenReturn("cn");
        when(searchOperation.execute(any(SearchRequest.class))).thenReturn(searchResponse);
        CDPGroup expectedGroup = new CDPGroup(cnGroup);

        var actualRes = ldapService.findGroupByName(cnGroup);

        assertTrue(actualRes.isRight());
        assertTrue(actualRes.get().isDefined());
        assertEquals(expectedGroup, actualRes.get().get());
    }

    @Test
    public void testFindGroupByNameWithNotExistingGroup() throws LdapException {
        var searchResponse = SearchResponse.builder().build();
        when(ldapConfig.groupSearchFilter()).thenReturn("(&(objectClass=groupOfNames)(cn={group}))");
        when(searchOperation.execute(any(SearchRequest.class))).thenReturn(searchResponse);

        var actualRes = ldapService.findGroupByName(cnGroup);

        assertTrue(actualRes.isRight());
        assertTrue(actualRes.get().isEmpty());
    }

    @Test
    public void testFindGroupByNameReturnError() throws LdapException {
        LdapException ex = new LdapException(ResultCode.TIME_LIMIT_EXCEEDED, "");
        when(ldapConfig.groupSearchFilter()).thenReturn("(&(objectClass=groupOfNames)(cn={group}))");
        when(searchOperation.execute(any(SearchRequest.class))).thenThrow(ex);
        String expectedDesc =
                "An error occurred while searching for the group 'group1' on LDAP. Please try again and if the error persists contact the platform team. Details: ";

        var actualRes = ldapService.findGroupByName(cnGroup);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertTrue(p.description().startsWith(expectedDesc));
            assertTrue(p.cause().isPresent());
            assertEquals(ex, p.cause().get());
        });
    }
}
