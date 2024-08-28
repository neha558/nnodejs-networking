import { Injectable, BadRequestException } from '@nestjs/common';
import { DatabaseService } from 'src/app/database/database.service';
import { UserWithdrawalStatus } from 'src/models/userWithdraw.entity';
import { In } from 'typeorm';

@Injectable()
export class UserWithdrawalsService {
  constructor(private readonly databaseService: DatabaseService) {}

  async userWithdrawalRepository() {
    return this.databaseService.getUserWithdrawalRepository();
  }

  async userRepository() {
    return this.databaseService.getUserRepository();
  }

  async createWithdrawalRequest(
    accountAddress: string,
    amount: number,
    toAddress: string,
  ) {
    const userRepo = await this.userRepository();
    const userWithdrawalRepo = await this.userWithdrawalRepository();

    const user = await userRepo.findOneOrFail({
      where: {
        accountAddress,
      },
    });

    if (user.totalWithdrawableAmount < amount) {
      throw new BadRequestException("You don't have enough amount to withdraw");
    }

    const existingReq = await userWithdrawalRepo.findOne({
      where: {
        user: {
          id: user?.id,
        },
        status: UserWithdrawalStatus.pending,
      },
    });

    if (existingReq?.id) {
      throw new BadRequestException(
        'You already have one open withdraw request, Please wait until its get resolve',
      );
    }

    const saved = await userWithdrawalRepo.save({
      user,
      toAddress,
      status: UserWithdrawalStatus.pending,
      txHash: '',
      amount,
    });

    // update totalWithdrawableAmount
    await userRepo.update(
      {
        id: user?.id,
      },
      {
        totalWithdrawableAmount:
          user.totalWithdrawableAmount - parseFloat(String(amount)),
      },
    );
    return saved;
  }

  async getWithdrawalRequest(accountAddress: string, query: any) {
    const userRepo = await this.userRepository();
    const userWithdrawalRepo = await this.userWithdrawalRepository();

    const user = await userRepo.findOneOrFail({
      where: {
        accountAddress,
      },
    });

    const [records, total] = await userWithdrawalRepo.findAndCount({
      where: {
        user: {
          id: user?.id,
        },
      },
      ...query,
    });
    return { records, total };
  }

  async getAllWithdrawalRequest(query: any) {
    const userWithdrawalRepo = await this.userWithdrawalRepository();

    const [records, total] = await userWithdrawalRepo.findAndCount({
      ...query,
    });
    return { records, total };
  }

  async updateWithdrawalRequest(id: number[], status: any) {
    const userWithdrawalRepo = await this.userWithdrawalRepository();

    await userWithdrawalRepo.update(
      {
        id: In(id),
        status: UserWithdrawalStatus.pending,
      },
      { status },
    );

    return true;
  }
}
