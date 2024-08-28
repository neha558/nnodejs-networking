import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { configService } from './app/config/config.service';
import { ScheduleModule } from '@nestjs/schedule';

import { ServeStaticModule } from '@nestjs/serve-static';
import { join } from 'path';

import { MailerModule } from '@nestjs-modules/mailer';

import { AppController } from './app.controller';
import { AppService } from './app.service';
import { DatabaseService } from './app/database/database.service';
import { UsersModule } from './app/modules/users/users.module';
import { MastersModule } from './app/modules/masters/masters.module';
import { VendorWalletServiceService } from './app/vendor-wallet-service/vendor-wallet-service.service';
import { Web3Service } from './app/web3/web3.service';
import { MailService } from './app/mail/mail.service';
@Module({
  imports: [
    TypeOrmModule.forRoot(configService.getTypeOrmConfig()),
    ScheduleModule.forRoot(),
    ServeStaticModule.forRoot({
      rootPath: join(__dirname, '../..', 'public'),
    }),
    UsersModule,
    MastersModule,
    MailerModule.forRoot({
      transport: {
        host: configService.getSMTPDetails().host,
        port: configService.getSMTPDetails().port,
        ignoreTLS: false,
        secure: false,
        auth: {
          user: configService.getSMTPDetails().user,
          pass: configService.getSMTPDetails().password,
        },
      },
      defaults: {
        from: `"${configService.getSMTPDetails().from}" <${
          configService.getSMTPDetails().fromEmail
        }>`,
      },
    }),
  ],
  controllers: [AppController],
  providers: [
    AppService,
    DatabaseService,
    VendorWalletServiceService,
    Web3Service,
    MailService,
  ],
})
export class AppModule {}
