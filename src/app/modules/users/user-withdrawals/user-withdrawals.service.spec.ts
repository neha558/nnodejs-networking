import { Test, TestingModule } from '@nestjs/testing';
import { UserWithdrawalsService } from './user-withdrawals.service';

describe('UserWithdrawalsService', () => {
  let service: UserWithdrawalsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [UserWithdrawalsService],
    }).compile();

    service = module.get<UserWithdrawalsService>(UserWithdrawalsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
