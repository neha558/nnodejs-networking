import { Injectable, Logger } from '@nestjs/common';
import { VendorWalletServiceService } from './app/vendor-wallet-service/vendor-wallet-service.service';
import { Cron } from '@nestjs/schedule';

@Injectable()
export class AppService {
  constructor(
    private readonly vendorWalletServiceService: VendorWalletServiceService,
  ) {}
  isUp(): string {
    return 'Hey there, I am up, Not to worry!!!';
  }

  async moralisWebHook(req, body) {
    try {
      this.vendorWalletServiceService.moralisWebhookUSDT(req, body);
      return true;
    } catch (error) {
      Logger.log(error);
    }
  }
}
