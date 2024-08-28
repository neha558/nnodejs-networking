import { Test, TestingModule } from '@nestjs/testing';
import { ImportOldUsersService } from './import-old-users.service';

describe('ImportOldUsersService', () => {
  let service: ImportOldUsersService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [ImportOldUsersService],
    }).compile();

    service = module.get<ImportOldUsersService>(ImportOldUsersService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
