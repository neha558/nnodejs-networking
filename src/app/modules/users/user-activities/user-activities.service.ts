import { Injectable } from '@nestjs/common';
import { DatabaseService } from 'src/app/database/database.service';

@Injectable()
export class UserActivitiesService {
  constructor(private readonly databaseService: DatabaseService) {}

  async userActivityRepository() {
    return this.databaseService.getUserActivityRepository();
  }

  async getUserActivities(accountAddress: string, take = '20', skip = '0') {
    const userActRepo = await this.userActivityRepository();
    const [records, total] = await userActRepo.findAndCount({
      where: {
        accountAddress: accountAddress?.toLowerCase(),
      },
      order: {
        createDateTime: 'DESC',
      },
      skip: parseInt(skip),
      take: parseInt(take),
    });

    return {
      records,
      total,
    };
  }
}
