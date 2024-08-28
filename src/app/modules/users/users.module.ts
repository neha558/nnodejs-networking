import { Module } from '@nestjs/common';
import { UsersController } from './users.controller';
import { UsersService } from './users.service';
import { DatabaseService } from 'src/app/database/database.service';
import { BonusService } from './bonus/bonus.service';
import { UserActivitiesService } from './user-activities/user-activities.service';
import { UserWithdrawalsService } from './user-withdrawals/user-withdrawals.service';
import { VendorWalletServiceService } from 'src/app/vendor-wallet-service/vendor-wallet-service.service';
import { JwtModule, JwtService } from '@nestjs/jwt';
import { Web3Service } from 'src/app/web3/web3.service';
import { configService } from 'src/app/config/config.service';
import { MailService } from 'src/app/mail/mail.service';
import { JwtStrategy } from './jwt.strategy';
import { ImportOldUsersService } from './import-old-users/import-old-users.service';

@Module({
  imports: [
    JwtModule.register({
      global: true,
      secret: `${configService.getJWTSecret()}`,
      signOptions: { expiresIn: '1d' },
    }),
  ],
  controllers: [UsersController],
  providers: [
    UsersService,
    DatabaseService,
    BonusService,
    UserActivitiesService,
    UserWithdrawalsService,
    VendorWalletServiceService,
    Web3Service,
    JwtStrategy,
    JwtService,
    MailService,
    ImportOldUsersService,
  ],
})
export class UsersModule {}
