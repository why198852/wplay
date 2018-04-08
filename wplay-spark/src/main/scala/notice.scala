/**
  * Created by james on 2017/6/6.
  *
  * 发送 Emai | DingDing
  *
  *
  */
package object notice {

  //  implicit def stringToSeq(single: String): Seq[String] = Seq(single)
  //
  //  implicit def liftToOption[T](t: T): Option[T] = Some(t)
  //
  //  sealed abstract class MailType
  //
  //  case object Plain extends MailType
  //
  //  case object Rich extends MailType
  //
  //  case object MultiPart extends MailType
  //
  //  case class Mail(from: (String, String) = ("bigdata_basic_monitor@wplay.cn", "程序拥堵告警"), // (email -> name)
  //                  to: Seq[String],
  //                  cc: Seq[String] = Seq.empty,
  //                  bcc: Seq[String] = Seq.empty,
  //                  subject: String,
  //                  message: String,
  //                  richMessage: Option[String] = None,
  //                  attachment: Option[(java.io.File)] = None)

  case class Ding(api: String, to: String, message: String)

  object send {
    /*def a(mail: Mail) {
      import java.util.Properties
      import javax.mail._

      import org.apache.commons.mail._

      val format =
        if (mail.attachment.isDefined) MultiPart
        else if (mail.richMessage.isDefined) Rich
        else Plain

      val commonsMail: Email = format match {
        case Plain => new SimpleEmail().setMsg(mail.message)
        case Rich => new HtmlEmail().setHtmlMsg(mail.richMessage.get).setTextMsg(mail.message)
        case MultiPart => {
          val attachment = new EmailAttachment()
          attachment.setPath(mail.attachment.get.getAbsolutePath)
          attachment.setDisposition(EmailAttachment.ATTACHMENT)
          attachment.setName(mail.attachment.get.getName)
          new MultiPartEmail().attach(attachment).setMsg(mail.message)
        }
      }

      val props: Properties = new Properties
      props.put("mail.smtp.auth", "true")
      props.put("mail.smtp.starttls.enable", "true")
      //不做服务器证书校验
      props.put("mail.smtp.ssl.checkserveridentity", "false")
      //添加信任的服务器地址，多个地址之间用空格分开
      props.put("mail.smtp.ssl.trust", "mail.wplay.cn")
      props.put("mail.smtp.host", "mail.wplay.cn")
      props.put("mail.smtp.port", "25")

      val session = Session.getInstance(props, new Authenticator() {
        override protected def getPasswordAuthentication =
          new PasswordAuthentication("bigdata_basic_monitor@wplay.intra", "YE@#0983")
      })

      commonsMail.setMailSession(session)

      mail.to foreach commonsMail.addTo
      mail.cc foreach commonsMail.addCc
      mail.bcc foreach commonsMail.addBcc


      commonsMail.
        setFrom(mail.from._1, mail.from._2).
        setSubject(mail.subject).
        send()

      println(s"Send Email ${mail.richMessage}")

    }*/

    def a(ding: Ding): Unit = {

      import scala.sys.process._

      //      val cmdStr = "https://oapi.dingtalk.com/robot/send?access_token=7f1bbc27fe224c1f285d108e8e794c062c0b14e2c304db8b0f8a59c806f62d5e"

      val body =
        s"""
           |{
           |  "msgtype": "text",
           |  "text": {
           |    "content": "${ding.message}"
           |  },
           |  "at": {
           |    "atMobiles": [
           |      "${ding.to}"
           |    ],
           |    "isAtAll": false
           |  }
           |}
        """.stripMargin

      val cmd = Seq("curl", "-s", "-L", "-X", "POST", "-H", "Content-Type: application/json", "-d " + body, ding.api)

      val result = cmd !!
    }
  }

}
