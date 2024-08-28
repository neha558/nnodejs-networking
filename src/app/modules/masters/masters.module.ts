import { Module } from '@nestjs/common';
import { MastersService } from './masters.service';
import { MastersController } from './masters.controller';
import { DatabaseService } from 'src/app/database/database.service';

@Module({
  providers: [MastersService, DatabaseService],
  controllers: [MastersController],
})
export class MastersModule {}
