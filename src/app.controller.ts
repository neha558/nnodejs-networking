import { Body, Controller, Get, Post, Req } from '@nestjs/common';
import { AppService } from './app.service';
import { Cron } from '@nestjs/schedule';
import { VendorWalletServiceService } from './app/vendor-wallet-service/vendor-wallet-service.service';
import { configService } from './app/config/config.service';

@Controller()
export class AppController {
  constructor(
    private readonly appService: AppService,
    private vendorWalletServiceService: VendorWalletServiceService,
  ) {}

  @Get('/up')
  isUp(): string {
    return this.appService.isUp();
  }

  // @Post('/moralis-webhook')
  // async moralisWebHook(@Req() req, @Body() body): Promise<any> {
  //   return this.appService.moralisWebHook(req, body);
  // }

  @Cron('* * * * *')
  async nftDistribution() {
    if (configService.getDisableCron()) {
      return;
    }
    this.vendorWalletServiceService.listenNFTDistribution();
    this.vendorWalletServiceService.getTokenTransferEvents();
    this.vendorWalletServiceService.listenUSDTRequests();
  }
}
