import { Injectable } from '@nestjs/common';
import { PaginatedDTO } from 'src/app/common/dto/paginated.dto';
import { DatabaseService } from 'src/app/database/database.service';

@Injectable()
export class MastersService {
  constructor(private readonly databaseService: DatabaseService) {}

  async getRanks(query: any): Promise<PaginatedDTO> {
    const rankRepo = await this.databaseService.getRankRepository();

    const [records, total] = await rankRepo.findAndCount({
      ...query,
    });

    return { records, total };
  }

  async getPacks(query: any): Promise<PaginatedDTO> {
    const packRepo = await this.databaseService.getPackRepository();

    const [records, total] = await packRepo.findAndCount({
      ...query,
      where: {
        isArchived: false,
      },
    });

    return { records, total };
  }
}
