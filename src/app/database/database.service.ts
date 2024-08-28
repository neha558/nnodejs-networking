import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { configService } from '../config/config.service';
import { User } from 'src/models/user.entity';
import { Config } from 'src/models/config.entity';
import { RefProfit } from 'src/models/refProfit.entity';
import { Rank } from 'src/models/rank.entity';
import { Pack } from 'src/models/pack.entity';
import { UserBonus } from 'src/models/userBonus.entity';
import { PackBought } from 'src/models/packBought.entity';
import { UserActivity } from 'src/models/userActivity.entity';
import { UserWithdrawal } from 'src/models/userWithdraw.entity';
import { ModuleWeb3WalletUser } from 'src/models/module_web3_wallet_users.entity';
import { ModuleWeb3USDTTransaction } from 'src/models/module_web3_wallet_usdt_transactions.entity';
import { UserBusiness } from 'src/models/userBusiness.entity';

export const AppDataSource = new DataSource(configService.getTypeOrmConfig());

const models = {
  User: User,
  Config: Config,
  RefProfit: RefProfit,
  Rank: Rank,
  Pack: Pack,
  UserBonus: UserBonus,
  PackBought: PackBought,
  UserActivity: UserActivity,
  UserWithdrawal: UserWithdrawal,
  ModuleWeb3WalletUser: ModuleWeb3WalletUser,
  ModuleWeb3USDTTransaction: ModuleWeb3USDTTransaction,
  UserBusiness: UserBusiness,
};

@Injectable()
export class DatabaseService {
  async getDataSource() {
    if (!AppDataSource.isInitialized) {
      await AppDataSource.initialize();
    }
    return AppDataSource;
  }

  async getUserRepository() {
    const dataSource = await this.getDataSource();
    return dataSource.getRepository(models.User);
  }

  async getRankRepository() {
    const dataSource = await this.getDataSource();
    return dataSource.getRepository(models.Rank);
  }

  async getPackRepository() {
    const dataSource = await this.getDataSource();
    return dataSource.getRepository(models.Pack);
  }

  async getUserBonusRepository() {
    const dataSource = await this.getDataSource();
    return dataSource.getRepository(models.UserBonus);
  }

  async getPackBoughtRepository() {
    const dataSource = await this.getDataSource();
    return dataSource.getRepository(models.PackBought);
  }

  async getUserActivityRepository() {
    const dataSource = await this.getDataSource();
    return dataSource.getRepository(models.UserActivity);
  }

  async getUserWithdrawalRepository() {
    const dataSource = await this.getDataSource();
    return dataSource.getRepository(models.UserWithdrawal);
  }

  async getModuleWeb3WalletUserRepository() {
    const dataSource = await this.getDataSource();
    return dataSource.getRepository(models.ModuleWeb3WalletUser);
  }

  async getModuleWeb3USDTTransactionRepository() {
    const dataSource = await this.getDataSource();
    return dataSource.getRepository(models.ModuleWeb3USDTTransaction);
  }

  async getUserBusinessRepository() {
    const dataSource = await this.getDataSource();
    return dataSource.getRepository(models.UserBusiness);
  }

  async getConfigRepository() {
    const dataSource = await this.getDataSource();
    return dataSource.getRepository(models.Config);
  }
}
