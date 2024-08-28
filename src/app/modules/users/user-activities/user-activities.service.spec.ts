import { Test, TestingModule } from '@nestjs/testing';
import { UserActivitiesService } from './user-activities.service';

describe('UserActivitiesService', () => {
  let service: UserActivitiesService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [UserActivitiesService],
    }).compile();

    service = module.get<UserActivitiesService>(UserActivitiesService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
