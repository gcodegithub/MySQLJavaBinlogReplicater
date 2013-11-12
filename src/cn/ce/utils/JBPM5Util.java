package cn.ce.utils;

//import java.util.Properties;
//
//import javax.persistence.EntityManager;
//import javax.persistence.EntityManagerFactory;
//
//import org.apache.commons.lang3.StringUtils;
//import org.drools.KnowledgeBase;
//import org.drools.KnowledgeBaseFactory;
//import org.drools.builder.KnowledgeBuilder;
//import org.drools.builder.KnowledgeBuilderError;
//import org.drools.builder.KnowledgeBuilderErrors;
//import org.drools.builder.KnowledgeBuilderFactory;
//import org.drools.builder.ResourceType;
//import org.drools.container.spring.beans.persistence.DroolsSpringJpaManager;
//import org.drools.container.spring.beans.persistence.DroolsSpringTransactionManager;
//import org.drools.impl.EnvironmentFactory;
//import org.drools.io.Resource;
//import org.drools.io.impl.ClassPathResource;
//import org.drools.persistence.PersistenceContextManager;
//import org.drools.persistence.TransactionManager;
//import org.drools.persistence.jpa.JPAKnowledgeService;
//import org.drools.runtime.Environment;
//import org.drools.runtime.EnvironmentName;
//import org.drools.runtime.KnowledgeSessionConfiguration;
//import org.drools.runtime.StatefulKnowledgeSession;
//import org.jbpm.process.workitem.wsht.HornetQHTWorkItemHandler;
//import org.springframework.orm.jpa.support.SharedEntityManagerBean;
//import org.springframework.transaction.support.AbstractPlatformTransactionManager;

public class JBPM5Util {
//
//	public static final KnowledgeBuilder kbuilder = KnowledgeBuilderFactory
//			.newKnowledgeBuilder();
//
//	public static void addFlowDefine(String bpmnFileClassPath) {
//		if (!StringUtils.isBlank(bpmnFileClassPath)) {
//			Resource r = new ClassPathResource(bpmnFileClassPath);
//			kbuilder.add(r, ResourceType.BPMN2);
//			if (kbuilder.hasErrors()) {
//				KnowledgeBuilderErrors errors = kbuilder.getErrors();
//				for (KnowledgeBuilderError error : errors) {
//					throw new IllegalStateException(error.getMessage());
//				}
//			}
//		}
//	}
//
//	public static StatefulKnowledgeSession initializeSession() {
//		KnowledgeBase kbase = kbuilder.newKnowledgeBase();
//		Properties p = new Properties();
//		p.put("drools.processInstanceManagerFactory",
//				"org.jbpm.process.instance.impl.DefaultProcessInstanceManagerFactory");
//		p.put("drools.processSignalManagerFactory",
//				"org.jbpm.process.instance.event.DefaultSignalManagerFactory");
//		KnowledgeSessionConfiguration conf = KnowledgeBaseFactory
//				.newKnowledgeSessionConfiguration(p);
//		StatefulKnowledgeSession ksession = kbase.newStatefulKnowledgeSession(
//				conf, null);
//		return ksession;
//	}
//
////	public static StatefulKnowledgeSession initializeSession(EntityManager em,
////			AbstractPlatformTransactionManager aptm) {
////		TransactionManager transactionManager = new DroolsSpringTransactionManager(
////				aptm);
////		Environment env = EnvironmentFactory.newEnvironment();
////		env.set(EnvironmentName.APP_SCOPED_ENTITY_MANAGER, em);
////		env.set(EnvironmentName.CMD_SCOPED_ENTITY_MANAGER, em);
////		env.set("IS_JTA_TRANSACTION", false);
////		env.set("IS_SHARED_ENTITY_MANAGER", true);
////		env.set(EnvironmentName.TRANSACTION_MANAGER, transactionManager);
////		PersistenceContextManager persistenceContextManager = new DroolsSpringJpaManager(
////				env);
////		env.set(EnvironmentName.PERSISTENCE_CONTEXT_MANAGER,
////				persistenceContextManager);
////		KnowledgeBase kbase = kbuilder.newKnowledgeBase();
////
////		StatefulKnowledgeSession ksession = JPAKnowledgeService
////				.newStatefulKnowledgeSession(kbase, null, env);
////		return ksession;
////	}
//
//	public static StatefulKnowledgeSession loadSessionBySID(EntityManager em,
//			AbstractPlatformTransactionManager aptm, int sid) {
//		TransactionManager transactionManager = new DroolsSpringTransactionManager(
//				aptm);
//		Environment env = EnvironmentFactory.newEnvironment();
//		env.set(EnvironmentName.APP_SCOPED_ENTITY_MANAGER, em);
//		env.set(EnvironmentName.CMD_SCOPED_ENTITY_MANAGER, em);
//		env.set("IS_JTA_TRANSACTION", false);
//		env.set("IS_SHARED_ENTITY_MANAGER", true);
//		env.set(EnvironmentName.TRANSACTION_MANAGER, transactionManager);
//		PersistenceContextManager persistenceContextManager = new DroolsSpringJpaManager(
//				env);
//		env.set(EnvironmentName.PERSISTENCE_CONTEXT_MANAGER,
//				persistenceContextManager);
//		KnowledgeBase kbase = kbuilder.newKnowledgeBase();
//		StatefulKnowledgeSession ksession = JPAKnowledgeService
//				.loadStatefulKnowledgeSession(sid, kbase, null, env);
//		return ksession;
//	}
//
//	public static HornetQHTWorkItemHandler getHornetQHandler(
//			StatefulKnowledgeSession ksession, String ip, int port) {
//		if (StringUtils.isBlank(ip) || port <= 0) {
//			throw new IllegalStateException(
//					"getHornetQHandler:ip or port is null");
//		}
//		HornetQHTWorkItemHandler humanTaskHandler = new HornetQHTWorkItemHandler(
//				ksession, true);
//		humanTaskHandler.setIpAddress(ip);
//		humanTaskHandler.setPort(port);
//		ksession.getWorkItemManager().registerWorkItemHandler("Human Task",
//				humanTaskHandler);
//		return humanTaskHandler;
//	}

}
