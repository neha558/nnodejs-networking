import { Controller, Get, Query } from '@nestjs/common';
import { AppRoutes } from 'src/app/common/constants/routes';
import { MastersService } from './masters.service';
import { IResponse } from 'src/app/common/interfaces/response.interface';
import { ResponseSuccess } from 'src/app/common/dto/response.dto';

@Controller(AppRoutes.masters)
export class MastersController {
  constructor(private readonly mastersService: MastersService) {}

  @Get('/ranks')
  async getRanks(@Query() query): Promise<IResponse> {
    const response = await this.mastersService.getRanks(query);
    return new ResponseSuccess('Ranks', response);
  }

  @Get('/packs')
  async getPacks(@Query() query): Promise<IResponse> {
    const response = await this.mastersService.getPacks(query);
    return new ResponseSuccess('Packs', response);
  }
}
