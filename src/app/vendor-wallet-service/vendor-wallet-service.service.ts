import { Injectable, Logger } from '@nestjs/common';
import { DatabaseService } from '../database/database.service';
import { Web3Service } from '../web3/web3.service';
import { ModuleWeb3WalletUser } from 'src/models/module_web3_wallet_users.entity';
import { configService } from '../config/config.service';
import axios from 'axios';
import { tokenABI } from '../common/constants/web3.constants';
import { In } from 'typeorm';
import { PackBoughtStatus } from 'src/models/packBought.entity';
import { UserWithdrawalStatus } from 'src/models/userWithdraw.entity';
const bcrypt = require('bcrypt');

const moralisAPI = configService.getMoralisAPIUrl();
const moralisAPIKey = configService.getMoralisAPIKey();
const contractAddress = configService.getTokenContractAddress();
const chain = configService.getChain();
const topics = {
  TOKEN_TRANSFER_USDT:
    '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
  BUY_PACK_EVENT:
    '0x89a537faaf5a85fddca8fdaee85fa956b604957efcdb61275c8511a55f855b45',
  WITHDRAW:
    '0x02f25270a4d87bea75db541cdfe559334a275b4a233520ed6c0a2429667cca94',
};

const moralisAPIPath = {
  txDetails: (tx) => `${moralisAPI}transaction/${tx}/verbose?chain=polygon`,
  boughtPacks: `${moralisAPI}${configService.getNFTDistributionContract()}/events?chain=${chain}&topic=${
    topics.BUY_PACK_EVENT
  }`,
  transferToken: `${moralisAPI}erc20/${contractAddress}/transfers?chain=${chain}`,
  withdrawRequests: `${moralisAPI}${configService.getUSDTWalletContract()}/events?chain=${chain}&topic=${
    topics.WITHDRAW
  }`,
};

const topicsABIs = {
  TOKEN_TRANSFER: {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'address',
        name: 'from',
        type: 'address',
      },
      {
        indexed: true,
        internalType: 'address',
        name: 'to',
        type: 'address',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'value',
        type: 'uint256',
      },
    ],
    name: 'Transfer',
    type: 'event',
  },
  BUY_PACK_EVENT: {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: 'address',
        name: 'userAddress',
        type: 'address',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'packId',
        type: 'uint256',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'id',
        type: 'uint256',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: 'Time',
        type: 'uint256',
      },
    ],
    name: 'BuyPackEvent',
    type: 'event',
  },
  WITHDRAW: {
    anonymous: false,
    inputs: [
      {
        indexed: false,
        internalType: 'address',
        name: '_address',
        type: 'address',
      },
      {
        indexed: false,
        internalType: 'uint256',
        name: '_amount',
        type: 'uint256',
      },
      { indexed: false, internalType: 'uint256', name: 'id', type: 'uint256' },
      {
        indexed: false,
        internalType: 'uint256',
        name: '_time',
        type: 'uint256',
      },
    ],
    name: 'Withdraw',
    type: 'event',
  },
};

const topicsABIsMap = {
  [topics.TOKEN_TRANSFER_USDT]: topicsABIs.TOKEN_TRANSFER,
  [topics.BUY_PACK_EVENT]: topicsABIs.BUY_PACK_EVENT,
};

@Injectable()
export class VendorWalletServiceService {
  constructor(
    private databaseService: DatabaseService,
    private web3Service: Web3Service,
  ) {}

  getModuleWeb3WalletUserRepo() {
    return this.databaseService.getModuleWeb3WalletUserRepository();
  }

  getModuleWeb3USDTTransactionRepo() {
    return this.databaseService.getModuleWeb3USDTTransactionRepository();
  }

  async getWalletUserDetails(accountAddress) {
    const userRepo = await this.getModuleWeb3WalletUserRepo();

    const userDetails = await userRepo.findOne({
      where: [
        {
          accountAddress: accountAddress?.toLowerCase(),
        },
        {
          accountAddress: Web3Service.web3.utils.toChecksumAddress(
            accountAddress?.toLowerCase(),
          ),
        },
      ],
    });
    return userDetails;
  }

  async checkEmailAndGetAccount(email: string): Promise<ModuleWeb3WalletUser> {
    const moduleWeb3WalletUser = await this.getModuleWeb3WalletUserRepo();
    const result = await moduleWeb3WalletUser.query(
      `SELECT id from user WHERE email='${email}'`,
    );
    if (result?.[0]?.id) {
      const _id = result?.[0]?.id;
      const existing = await moduleWeb3WalletUser.findOne({
        where: {
          _id,
        },
      });

      return {
        ...existing,
        accountAddress: existing?.accountAddress?.toLowerCase(),
        walletAddress: existing?.accountAddress?.toLowerCase(),
      };
    }

    return null;
  }

  async updateUSDTBalance(accountAddress: string, usdtBalance: number) {
    const userRepo = await this.getModuleWeb3WalletUserRepo();
    const userDetails = await userRepo.findOne({
      where: [
        {
          accountAddress: accountAddress?.toLowerCase(),
          isActive: false,
        },
        {
          accountAddress: Web3Service.web3.utils.toChecksumAddress(
            accountAddress?.toLowerCase(),
          ),
          isActive: false,
        },
      ],
    });

    if (userDetails?.id) {
      await userRepo.update(
        {
          id: userDetails.id,
        },
        {
          totalUSDTBalance: userDetails.totalUSDTBalance ?? 0 + usdtBalance,
        },
      );
    }
  }

  async setPasswordOfMainTable(email: string, passwordGiven: string) {
    const userRepo = await this.getModuleWeb3WalletUserRepo();
    const result = await userRepo.query(
      `SELECT id from user WHERE email='${email}'`,
    );
    if (result?.[0]?.id && passwordGiven) {
      const password = bcrypt.hashSync(passwordGiven, bcrypt.genSaltSync(9));
      await userRepo.query(
        "UPDATE `user` SET password_hash='" +
          password +
          "' WHERE `id`='" +
          result?.[0]?.id +
          "';",
      );
    }
  }

  async checkEmailAndAccountOfWallet(
    email: string,
    passwordGiven: string,
    exists,
  ): Promise<ModuleWeb3WalletUser> {
    const userRepo = await this.getModuleWeb3WalletUserRepo();
    const moduleWeb3WalletUser = await this.getModuleWeb3WalletUserRepo();

    let _id;

    if (!exists?.id) {
      const result = await userRepo.query(
        `SELECT id from user WHERE email='${email}'`,
      );
      if (result?.[0]?.id) {
        _id = result?.[0]?.id;
      } else {
        const password = bcrypt.hashSync(
          passwordGiven ||
            String(Math.floor(Math.random() * 9000000000) + 1000000000),
          bcrypt.genSaltSync(9),
        );
        await userRepo.query(
          "INSERT INTO `user` ( `username`, `email`, `is_email_verified`, `wallet_address`, `auth_key`, `password_hash` ) VALUES ('" +
            email +
            "','" +
            email +
            "'," +
            "false, '', '','" +
            password +
            "');",
        );
        const result = await userRepo.query(
          `SELECT id from user WHERE email='${email}'`,
        );
        if (result?.[0]?.id) {
          _id = result?.[0]?.id;
        }
      }
    }

    const userDetails = await userRepo.findOne({
      where: [
        {
          accountAddress: exists?.accountAddress?.toLowerCase(),
          isActive: false,
        },
        {
          accountAddress: Web3Service.web3.utils.toChecksumAddress(
            exists?.accountAddress?.toLowerCase(),
          ),
          isActive: false,
        },
      ],
    });

    if (userDetails?.id) {
      return userDetails;
    }

    const accountDetails = this.web3Service.createAccount();
    const existing = await moduleWeb3WalletUser.findOne({
      where: {
        _id,
        accountAddress: accountDetails?.address,
      },
    });

    if (existing?.accountAddress) {
      return existing;
    }

    const saved = await moduleWeb3WalletUser.save({
      accountAddress: accountDetails?.address,
      encrypted: accountDetails?.encrypted,
      _id,
      status: 'created',
      id: existing?.id,
      nonce: Math.floor(Math.random() * 1000000),
      role: 'user',
    });

    await userRepo.query(
      "UPDATE `user` SET wallet_address='" +
        saved?.accountAddress +
        "' WHERE `id`='" +
        _id +
        "';",
    );

    return saved;
  }

  async deductUSDTAmount(
    accountAddress: string,
    amount: number,
    description = 'Pack bought direct deduct',
  ) {
    const USDTTransactionRepo = await this.getModuleWeb3USDTTransactionRepo();
    const userRepo = await this.getModuleWeb3WalletUserRepo();

    const saved = await USDTTransactionRepo.save({
      accountAddress,
      description,
      amount: String(amount),
      status: 'success',
      operation: 'deduct',
      transactionHash: Math.random().toString(),
    });

    const userDetails = await userRepo.findOne({
      where: [
        {
          accountAddress: accountAddress?.toLowerCase(),
        },
        {
          accountAddress: Web3Service.web3.utils.toChecksumAddress(
            accountAddress?.toLowerCase(),
          ),
        },
      ],
    });

    await userRepo.update(
      {
        id: userDetails?.id,
      },
      {
        totalUSDTBalance: (userDetails?.totalUSDTBalance ?? 0) - amount,
      },
    );

    return saved;
  }

  verifySignature = (req) => {
    const providedSignature = req.headers['x-signature'];
    if (!providedSignature) {
      Logger.log('Signature not provided');
      return false;
    }

    const generatedSignature = Web3Service.web3.utils.sha3(
      JSON.stringify(req.body) + configService.getMoralisSecret(),
    );

    console.log('sign', generatedSignature, providedSignature);
    if (generatedSignature !== providedSignature) {
      Logger.log('Signature is not valid');
      return false;
    }
    return true;
  };

  // USDT
  async moralisWebhookUSDT(req, data) {
    try {
      if (!this.verifySignature(req)) {
        return false;
      }
      const confirmed = data?.confirmed;

      if (!confirmed) {
        return;
      }

      const userRepo = await this.getModuleWeb3WalletUserRepo();

      const users = await userRepo.find({
        select: ['accountAddress'],
      });

      const savedAccounts = users?.map((_u) =>
        String(_u.accountAddress).toLowerCase(),
      );

      const logs = data?.logs;

      for (let index = 0; index < logs.length; index++) {
        const log = logs[index];
        const blockNumber = data?.block?.number;

        if (!confirmed) {
          return;
        }

        if (log?.topic0 === topics.TOKEN_TRANSFER_USDT) {
          const erc20Transfers = data?.erc20Transfers;
          if (erc20Transfers?.length > 0) {
            const records = erc20Transfers
              ?.map((eventData) => {
                return {
                  ...eventData,
                  block_number: blockNumber,
                  to_address: eventData?.to,
                  from_address: eventData?.from,
                  transaction_hash: eventData?.transactionHash,
                  log,
                };
              })
              .filter((_d) => {
                return savedAccounts.includes(_d?.to_address?.toLowerCase());
              });

            Logger.log('Deposit records ', JSON.stringify(records));
            if (records.length > 0) {
              Logger.log(['Moralis webhook USDT', JSON.stringify(log)]);
              await this.saveTokenTransactionUSDT(records);
              await this.holdReceivedAmountUSDT();
            }

            continue;
          }
          continue;
        }
      }
    } catch (error) {
      Logger.log('Error in hook');
      console.log(error);
    }
  }

  async saveTokenTransactionUSDT(data) {
    try {
      const USDTTransactionRepo = await this.getModuleWeb3USDTTransactionRepo();

      for (const d of data) {
        try {
          await USDTTransactionRepo.save({
            accountAddress: d?.to_address,
            operation: 'deposited',
            status: 'received',
            blockchainTxId: d?.transaction_hash,
            transactionHash: d?.transaction_hash,
            amount: d?.valueWithDecimals,
            toAccountAddress: d?.to_address,
          });
        } catch (error) {
          Logger.log([
            '=========== INVALID TX ============',
            d?.transaction_hash,
          ]);
          Logger.log([
            'saveTokenTransactionError',
            JSON.stringify(error?.message),
          ]);
        }
      }
    } catch (error) {
      Logger.log(['saveTokenTransactionError', JSON.stringify(error?.message)]);
    }
  }

  async holdReceivedAmountUSDT() {
    try {
      const USDTTransactionRepo = await this.getModuleWeb3USDTTransactionRepo();
      const UserRepo = await this.getModuleWeb3WalletUserRepo();

      const receivedAmountForUsers = await USDTTransactionRepo.find({
        where: {
          status: 'received',
        },
      });
      Logger.log('holdReceivedAmountUSDT Every minute');

      for (const receivedAmountForUser of receivedAmountForUsers) {
        await USDTTransactionRepo.update(
          {
            id: receivedAmountForUser?.id,
          },
          {
            status: 'selected_for_deposit',
          },
        );

        const user = await UserRepo.findOne({
          where: [
            {
              accountAddress:
                receivedAmountForUser?.accountAddress?.toLowerCase(),
            },
            {
              accountAddress: Web3Service.web3.utils.toChecksumAddress(
                receivedAmountForUser?.accountAddress?.toLowerCase(),
              ),
            },
          ],
        });

        if (!user?.accountAddress) {
          continue;
        }
        Logger.log(
          'holdReceivedAmountUSDT ' +
            receivedAmountForUser?.accountAddress +
            ': ' +
            receivedAmountForUser?.amount,
        );

        await UserRepo.update(
          {
            accountAddress: receivedAmountForUser?.accountAddress,
          },
          {
            totalUSDTBalance: Number(
              (user?.totalUSDTBalance ?? 0) + receivedAmountForUser?.amount,
            ),
          },
        );

        const receipt = await this.transferToWallet(
          receivedAmountForUser?.accountAddress,
        );

        await USDTTransactionRepo.update(
          {
            id: receivedAmountForUser?.id,
          },
          {
            status: 'sent_currency',
            transactionHashMatic: receipt?.transactionHash,
          },
        );

        await this.transferUSDTToContract(
          user?.accountAddress,
          receivedAmountForUser?.amount,
          user?.encrypted,
          receivedAmountForUser?.id,
          receivedAmountForUser?.id,
        );
      }
      return receivedAmountForUsers;
    } catch (error) {
      Logger.log(['Error in holding', JSON.stringify(error?.message)]);
    }
  }

  async transferUSDTToContract(
    address: string,
    amount: string,
    encrypted: string,
    tokenTxId: number,
    txId: number,
  ) {
    try {
      Logger.log(
        '+++++++++++++++++++++++++++++++ Transfer USDT TO Contract Started +++++++++++++++++++++++++++++++',
      );
      const USDTTransactionRepo = await this.getModuleWeb3USDTTransactionRepo();

      const tokenContract = this.getTokenContractAddress(address);

      const encodedABI = tokenContract.methods
        .transfer(
          configService.getPackContractAddress(),
          Web3Service.web3.utils.toWei(
            `${amount}`,
            configService.getDecimals(),
          ),
        )
        .encodeABI();

      const dataForTx = {
        to: configService.getTokenContractAddress(),
        from: address,
        gas: 50000,
        data: encodedABI,
      };

      const signedTx = await Web3Service.web3.eth.accounts.signTransaction(
        dataForTx,
        this.decryptPrivateKey(encrypted)?.privateKey,
      );

      Logger.log(
        '+++++++++++++++++++++++++++++++ Transfer USDT TO Contract +++++++++++++++++++++++++++++++',
      );
      Logger.log([
        'dataForTx',
        JSON.stringify(dataForTx),
        JSON.stringify(signedTx),
      ]);

      const receipt = await Web3Service.web3.eth.sendSignedTransaction(
        signedTx?.rawTransaction,
      );

      Logger.log(
        '+++++++++++++++++++++++++++++++ Transfer USDT TO Contract Receipt +++++++++++++++++++++++++++++++',
      );

      Logger.log(['receipt', JSON.stringify(receipt)]);

      await USDTTransactionRepo.update(
        {
          id: txId,
        },
        {
          blockchainTxId: receipt?.transactionHash,
          status: 'success',
        },
      );

      return receipt;
    } catch (error) {
      Logger.log(
        '+++++++++++++++++++++++++++++++ Transfer USDT TO Contract error +++++++++++++++++++++++++++++++',
      );
      console.log(error);
      this.setErrorFailedWhileTransfer(tokenTxId);
    }
  }

  getPackContractAddress(address) {
    const packContract = new Web3Service.web3.eth.Contract(
      tokenABI,
      configService.getPackContractAddress(),
      {
        from: address,
      },
    );
    return packContract;
  }

  getTokenContractAddress(address) {
    const contract = new Web3Service.web3.eth.Contract(
      tokenABI,
      configService.getTokenContractAddress(),
      {
        from: address,
      },
    );
    return contract;
  }

  decryptPrivateKey(encrypted) {
    return Web3Service.web3.eth.accounts.decrypt(
      encrypted,
      configService.getEncryptionKey(),
    );
  }

  async setErrorFailedWhileTransfer(tokenTxId: number) {
    const USDTTransactionRepo = await this.getModuleWeb3USDTTransactionRepo();
    await USDTTransactionRepo.update(
      {
        id: tokenTxId,
      },
      {
        status: 'hold_failed',
      },
    );
  }

  async getLatestBlockGasFee() {
    const response = await axios.get(
      `https://gasstation.polygon.technology/v2`,
    );

    return {
      matic: Web3Service.web3.utils.toWei('0.02'),
      gwe: String(response?.data?.standard?.maxFee),
    };
    // 1 MATIC - 1000000000 GWE
    // x MATIC - 243.59189772766666 GWE
  }

  async transferToWallet(address: string) {
    const gasPrice = await this.getLatestBlockGasFee();

    const transactionParameters = {
      from: configService.getDepositorAddress(),
      to: address,
      data: '0x',
      value: gasPrice?.matic,
      gas: 500000,
      gasFee: gasPrice?.gwe,
    };

    const signedTx = await Web3Service.web3.eth.accounts.signTransaction(
      transactionParameters,
      configService.getDepositorPrivateKey(),
    );

    Logger.log(
      '+++++++++++++++++++++++++++++++ Transfer MATIC to wallet +++++++++++++++++++++++++++++++',
    );

    Logger.log([
      'transferMaticToWallet',
      JSON.stringify(transactionParameters),
      JSON.stringify(signedTx),
    ]);

    const receipt = await Web3Service.web3.eth.sendSignedTransaction(
      signedTx?.rawTransaction,
    );

    Logger.log(
      '+++++++++++++++++++++++++++++++ Transfer MATIC to wallet reciept +++++++++++++++++++++++++++++++',
    );

    Logger.log(['receipt', JSON.stringify(receipt)]);

    return receipt;
  }
  // USDT done

  // NFT PACK DISTRIBUTION
  async listenNFTDistribution() {
    try {
      const configRepo = await this.databaseService.getConfigRepository();

      const fromBlock = await configRepo.findOne({
        where: {
          name: 'NFT_DISTRIBUTION_FROM_BLOCK',
        },
      });

      let from_block = configService.getNFTDistributionContractBlock();

      if (fromBlock) {
        from_block = fromBlock?.value;
      }

      let response;
      let records = [];

      let page = 1;
      do {
        const url = `${
          moralisAPIPath.boughtPacks
        }&from_block=${from_block}&offset=${(page - 1) * 100}`;

        response = await axios.post(url, topicsABIs.BUY_PACK_EVENT, {
          headers: {
            'X-API-Key': moralisAPIKey,
          },
        });

        console.log([url, JSON.stringify(response?.data)]);

        if (page === 1 && response?.data?.result?.[0]?.block_number) {
          await configRepo.save({
            id: fromBlock?.id,
            name: 'NFT_DISTRIBUTION_FROM_BLOCK',
            value: String(
              parseInt(response?.data?.result?.[0]?.block_number) + 1,
            ),
          });
        }
        page++;

        records = [...records, ...response?.data?.result];
      } while (response?.data?.result?.length > 0);

      const boughtPackRepo =
        await this.databaseService.getPackBoughtRepository();

      await Promise.all(
        records?.map((record) => {
          return boughtPackRepo.update(
            {
              id: parseInt(record?.data?.uid, 10),
            },
            {
              status: PackBoughtStatus.nftDistributed,
              txHash: record?.transaction_hash,
            },
          );
        }),
      );
    } catch (error) {
      Logger.log('+++++++++ ERROR IN MORALIS API');
      console.log(error);
      Logger.log('+++++++++ ERROR IN MORALIS API');
    }
  }
  // NFT PACK DISTRIBUTION DONE

  // API DRIVEN TRANSFER
  async getTokenTransferEvents() {
    try {
      const userRepo = await this.databaseService.getUserRepository();
      const configRepo = await this.databaseService.getConfigRepository();
      const fromBlock = await configRepo.findOne({
        where: {
          name: 'TRANSFER_TOKEN',
        },
      });

      let from_block = '44434845';

      if (fromBlock) {
        from_block = fromBlock?.value;
      }

      let response;
      let records = [];

      let page = 1;
      let cursor;
      do {
        let url = `${
          moralisAPIPath.transferToken
        }&from_block=${from_block}&offset=${
          (page - 1) * 100
        }&disable_total=true&cacheBuster=${new Date().getTime()}`;

        if (cursor) {
          url = url + `&cursor=${cursor}`;
        }

        response = await axios.get(url, {
          headers: {
            'X-API-Key': moralisAPIKey,
          },
        });
        Logger.log([
          url,
          JSON.stringify(
            response?.data?.result?.map((_result) => _result?.transaction_hash),
          ),
        ]);

        if (page === 1 && response?.data?.result?.[0]?.block_number) {
          await configRepo.save({
            id: fromBlock?.id,
            name: 'TRANSFER_TOKEN',
            value: String(
              parseInt(response?.data?.result?.[0]?.block_number) + 1,
            ),
          });
          page++;
        }

        records = [...records, ...response?.data?.result];
        cursor = response?.data?.cursor;
      } while (response?.data?.cursor);

      if (records?.length > 0) {
        const userAddress = await userRepo.find({
          select: ['accountAddress'],
        });

        const existingUserAddress = userAddress?.map((_userAddress) =>
          _userAddress?.accountAddress?.toLowerCase(),
        );

        Logger.log('Existing users: ' + existingUserAddress.length);
        if (existingUserAddress?.length <= 0) {
          return false;
        }

        records = records
          .filter((_d) => {
            return existingUserAddress?.includes(_d?.to_address);
          })
          .map((_record) => {
            return {
              from_address: _record?.from_address,
              to_address: _record?.to_address,
              valueWithDecimals: _record?.value_decimal,
              value: _record?.value,
              transaction_hash: _record?.transaction_hash,
            };
          });

        console.log(
          'records',
          records?.map((_record) => _record?.transaction_hash),
        );
        if (records?.length > 0) {
          await this.saveTokenTransactionUSDT(records);
        }
      }

      return records;
    } catch (error) {
      Logger.log('Error in moralis API');
      console.log(error);
    }
  }
  // API DRIVEN TRANSFER MANAGE

  // USDT Withdraw request
  async listenUSDTRequests() {
    try {
      const configRepo = await this.databaseService.getConfigRepository();

      const fromBlock = await configRepo.findOne({
        where: {
          name: 'USDT_REQUEST_FROM_BLOCK',
        },
      });

      let from_block = '0';

      if (fromBlock) {
        from_block = fromBlock?.value;
      }

      let response;
      let records = [];

      let page = 1;
      do {
        const url = `${
          moralisAPIPath.withdrawRequests
        }&from_block=${from_block}&offset=${(page - 1) * 100}`;

        response = await axios.post(url, topicsABIs.WITHDRAW, {
          headers: {
            'X-API-Key': moralisAPIKey,
          },
        });

        console.log([url, JSON.stringify(response?.data)]);

        if (page === 1 && response?.data?.result?.[0]?.block_number) {
          await configRepo.save({
            id: fromBlock?.id,
            name: 'USDT_REQUEST_FROM_BLOCK',
            value: String(
              parseInt(response?.data?.result?.[0]?.block_number) + 1,
            ),
          });
        }
        page++;

        records = [...records, ...response?.data?.result];
      } while (response?.data?.result?.length > 0);

      const withdrawRequestRepo =
        await this.databaseService.getUserWithdrawalRepository();

      await Promise.all(
        records?.map((record) => {
          return withdrawRequestRepo.update(
            {
              id: parseInt(record?.data?.uid, 10),
            },
            {
              status: UserWithdrawalStatus.success,
              txHash: record?.transaction_hash,
            },
          );
        }),
      );
    } catch (error) {
      Logger.log('+++++++++ ERROR IN MORALIS API');
      console.log(error);
      Logger.log('+++++++++ ERROR IN MORALIS API');
    }
  }
  // USDT Withdraw request
}
