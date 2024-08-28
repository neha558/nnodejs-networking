import { Injectable } from '@nestjs/common';
import { configService } from '../config/config.service';

const Web3 = require('web3');

const web3 = new Web3('https://polygon-rpc.com/');

@Injectable()
export class Web3Service {
  static web3 = web3;

  createAccount() {
    const account = web3.eth.accounts.create();
    const encrypted = web3.eth.accounts.encrypt(
      account?.privateKey,
      configService.getEncryptionKey(),
    );

    return {
      address: account?.address,
      encrypted: JSON.stringify(encrypted),
    };
  }
}
