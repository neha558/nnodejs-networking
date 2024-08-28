import { Test, TestingModule } from '@nestjs/testing';
import { VendorWalletServiceService } from './vendor-wallet-service.service';

describe('VendorWalletServiceService', () => {
  let service: VendorWalletServiceService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [VendorWalletServiceService],
    }).compile();

    service = module.get<VendorWalletServiceService>(VendorWalletServiceService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
