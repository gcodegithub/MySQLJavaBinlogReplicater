package cn.ce.utils.mail;

/**
 * This class is used to send simple internet email messages without
 * attachments.
 * 
 * @since 1.0
 * @author <a href="mailto:quintonm@bellsouth.net">Quinton McCombs</a>
 * @author <a href="mailto:colin.chalmers@maxware.nl">Colin Chalmers</a>
 * @author <a href="mailto:jon@latchkey.com">Jon S. Stevens</a>
 * @author <a href="mailto:frank.kim@clearink.com">Frank Y. Kim</a>
 * @author <a href="mailto:bmclaugh@algx.net">Brett McLaughlin</a>
 * @author <a href="mailto:unknown">Regis Koenig</a>
 * @version $Id: SimpleEmail.java,v 1.2 2011/04/19 06:17:18 wangjing Exp $
 */
public class SimpleEmail extends Email {
	/**
	 * Set the content of the mail
	 * 
	 * @param msg
	 *            A String.
	 * @return An Email.
	 * @throws EmailException
	 *             see javax.mail.internet.MimeBodyPart for definitions
	 * @since 1.0
	 */

	private String endFix = ";charset=UTF-8";

	public SimpleEmail() {

	}

	public SimpleEmail(String endFix) {
		this.endFix = endFix;
	}

	public Email setMsg(String msg) throws EmailException {
		if (EmailUtils.isEmpty(msg)) {
			throw new EmailException("Invalid message supplied");
		}

		setContent(msg, TEXT_PLAIN + endFix);
		return this;
	}
}
