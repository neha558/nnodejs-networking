import { Injectable, Logger } from '@nestjs/common';
import { MailerService } from '@nestjs-modules/mailer';
import { configService } from '../config/config.service';

@Injectable()
export class MailService {
  constructor(private readonly mailerService: MailerService) {}

  sendMail(to: string, subject: string, html: string) {
    if (configService.getByPassEmailVerification()) {
      return Promise.resolve();
    }
    this.mailerService
      .sendMail({
        to,
        subject,
        html,
        from: configService.getSMTPDetails().fromEmail,
      })
      .then(() => {
        Logger.log('Mail sent: ' + subject + ', To: ' + to);
      })
      .catch((err) => {
        Logger.log(err);
      });
  }
}
