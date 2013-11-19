package cn.ce.utils.mail;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.utils.common.BeanUtil;


public class Alarm {
	private static final Log logger = LogFactory.getLog(Alarm.class);

	public static void sendFetalErrorEmail(String smtpPort, String smtpAdd,
			String sendEmailAdd, String sendEmailPass, String recEmailAddCSV,
			String csvToken, String subject, String emailContent) {
		try {
			logger.info("准备发送报警邮件");
			Email email = new SimpleEmail();
			email.setCharset("UTF-8");
			email.setSmtpPort(Integer.valueOf(smtpPort));
			email.setAuthenticator(new DefaultAuthenticator(sendEmailAdd,
					sendEmailPass));
			email.setDebug(false);
			email.setHostName(smtpAdd);
			List<String> recAddList = BeanUtil.csvToList(recEmailAddCSV,
					csvToken);
			for (String recEmailAdd : recAddList) {
				email.addTo(recEmailAdd);
			}
			email.setFrom(sendEmailAdd);
			email.setSubject(subject);
			email.setMsg(emailContent);

			email.send();
			logger.info("发送报警邮件给:" + recEmailAddCSV);
			logger.info("邮件内容为:" + emailContent);
		} catch (Throwable e) {
			e.printStackTrace();
			logger.error("发送报警邮件失败，邮件内容:" + emailContent);
		}
	}

	public static void main(String[] args) throws Exception {

		for (int i = 0; i < 3; i++) {
			// String smtpPort, String smtpAdd,String sendEmailAdd, String
			// sendEmailPass, String recEmailAdd,String subject, String
			// emailContent
			Alarm.sendFetalErrorEmail("25", "mail.300.cn", "szswb@300.cn",
					"q1w2e3r4ys",
					"yaochuncen@300.cn,yaochuncen@300.cn,yaochuncen@300.cn",
					",", "Hello 多个收件人测试", "中文的内容是否乱码");
			System.out.println(i);
			Thread.sleep(3000);
		}
		System.out.println("-----------------OVER------------------");
	}
}
