import {
  Controller,
  Post,
  Body,
  Param,
  Get,
  Query,
  Put,
  UseGuards,
  Req,
} from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';
import { AppRoutes } from 'src/app/common/constants/routes';
import { IResponse } from 'src/app/common/interfaces/response.interface';
import { UsersService } from './users.service';
import { ResponseSuccess } from 'src/app/common/dto/response.dto';
import { UserRegisterDTO } from './dto/UserRegisterDTO.dto';
import { BonusService } from './bonus/bonus.service';
import { UserActivitiesService } from './user-activities/user-activities.service';
import { UserWithdrawalsService } from './user-withdrawals/user-withdrawals.service';
import { ImportOldUsersService } from './import-old-users/import-old-users.service';

@Controller(AppRoutes.users)
export class UsersController {
  constructor(
    private readonly usersService: UsersService,
    private readonly bonusService: BonusService,
    private readonly userActivitiesService: UserActivitiesService,
    private readonly userWithdrawalsService: UserWithdrawalsService,
    private readonly importOldUsersService: ImportOldUsersService,
  ) {}

  @Post('/register')
  async register(@Body() userRegisterDTO: UserRegisterDTO): Promise<IResponse> {
    const response = await this.usersService.register(userRegisterDTO);
    return new ResponseSuccess('User Register', response);
  }

  @Post('/login')
  async login(@Body() body): Promise<IResponse> {
    const response = await this.usersService.login(body);
    return new ResponseSuccess('User Login', response);
  }

  @Put('/reset-password')
  async resetPassword(@Body() body): Promise<IResponse> {
    const response = await this.usersService.resetPassword(body?.email);
    return new ResponseSuccess('Reset Password', response);
  }

  @Put('/set-password')
  async setPassword(@Body() body): Promise<IResponse> {
    const response = await this.usersService.setPassword(
      body?.token,
      body?.password,
    );
    return new ResponseSuccess('Set Password', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Put('/acceptTerms')
  async acceptTerms(@Req() req): Promise<IResponse> {
    const response = await this.usersService.acceptTerms(
      req?.user?.accountAddress,
    );
    return new ResponseSuccess('User Accept terms', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Get('/details')
  async details(@Req() req, @Query() query): Promise<IResponse> {
    const response = await this.usersService.details(
      req?.user?.accountAddress,
      query,
    );
    return new ResponseSuccess('User details', response);
  }

  @Get('/details-based-on-code')
  async detailsBasedOnCode(@Query() query): Promise<IResponse> {
    const response = await this.usersService.detailsBasedOnCode(query?.code);
    return new ResponseSuccess('User details', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Get('/bonus')
  async bonus(@Req() req, @Query() query): Promise<IResponse> {
    const response = await this.bonusService.getUserBonus(
      req?.user?.accountAddress,
      query,
    );
    return new ResponseSuccess('User bonus', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Get('/packs')
  async packs(@Req() req, @Query() query): Promise<IResponse> {
    const response = await this.bonusService.getUserPacks(
      req?.user?.accountAddress,
      query,
    );
    return new ResponseSuccess('User bonus', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Get('/partners')
  async getPartners(@Req() req, @Query() query): Promise<IResponse> {
    const response = await this.usersService.getPartners(
      req?.user?.accountAddress,
      query?.take,
      query?.skip,
    );
    return new ResponseSuccess('User partners', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Get('/activities')
  async getUserActivities(@Req() req, @Query() query): Promise<IResponse> {
    const response = await this.userActivitiesService.getUserActivities(
      req?.user?.accountAddress,
      query?.take,
      query?.skip,
    );
    return new ResponseSuccess('User activities', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Get('/withdrawal')
  async getWithdrawalRequest(@Req() req, @Query() query): Promise<IResponse> {
    const response = await this.userWithdrawalsService.getWithdrawalRequest(
      req?.user?.accountAddress,
      query,
    );
    return new ResponseSuccess('User withdrawal', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Get('/withdrawal-all')
  async getAllWithdrawalRequest(@Query() query): Promise<IResponse> {
    const response = await this.userWithdrawalsService.getAllWithdrawalRequest(
      query,
    );
    return new ResponseSuccess('User withdrawal allF', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Post('/withdrawal')
  async createWithdrawalRequest(@Req() req, @Body() body): Promise<IResponse> {
    const response = await this.userWithdrawalsService.createWithdrawalRequest(
      req?.user?.accountAddress,
      body?.amount,
      body?.toAddress,
    );
    return new ResponseSuccess('User withdrawal', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Put('/withdrawal')
  async updateWithdrawalRequest(@Body() body): Promise<IResponse> {
    this.usersService.validateSignatureOfAuthorizedPerson(
      body,
      body?.signature,
    );
    const response = await this.userWithdrawalsService.updateWithdrawalRequest(
      body?.id,
      body?.status,
    );
    return new ResponseSuccess('User withdrawal update', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Post('/buy-pack')
  async distributeProfit(@Req() req, @Body() body): Promise<IResponse> {
    const response = await this.usersService.buyPack({
      ...body,
      accountAddress: req?.user?.accountAddress,
    });
    return new ResponseSuccess('Buy pack', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Get('/bought-pack')
  async getAllUndistributedPacks(@Query() query): Promise<IResponse> {
    const response = await this.bonusService.getAllUndistributedPacks(query);
    return new ResponseSuccess('Bought packs', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Get('/get-ids-based-on-email')
  async getIdsBasedOnEmail(@Req() req): Promise<IResponse> {
    const response = await this.usersService.getIdsBasedOnEmail(
      req?.user?.email,
    );
    return new ResponseSuccess('Get Ids based on email', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Post('/change-user-id/:accountAddress')
  async changeUserId(@Req() req, @Param() params): Promise<IResponse> {
    const response = await this.usersService.changeUserId(
      req?.user?.email,
      params?.accountAddress,
    );
    return new ResponseSuccess('Account address', response);
  }

  @Get('/partners-tree/:accountAddress')
  async getPartnersTree(@Param() params): Promise<IResponse> {
    const response = await this.usersService.getPartnersTree(
      params?.accountAddress,
    );
    return new ResponseSuccess('Partner tree', response);
  }

  // @Get('/old-data')
  // async registerOldUsers(): Promise<IResponse> {
  //   const response = await this.importOldUsersService.registerOldUsers();
  //   return new ResponseSuccess('Old Data', response);
  // }

  // @Get('/old-pack-data')
  // async buyOldUserPack(): Promise<IResponse> {
  //   const response = await this.importOldUsersService.buyOldUserPack();
  //   return new ResponseSuccess('Old Data', response);
  // }

  // @Get('/sync-user-data')
  // async syncUserBusiness(): Promise<IResponse> {
  //   const response = await this.importOldUsersService.syncUserBusiness();
  //   return new ResponseSuccess('Old Data', response);
  // }

  @Post('/set-email')
  async setEmail(@Body() body): Promise<IResponse> {
    const response = await this.usersService.setEmail(body);
    return new ResponseSuccess('Set Email', response);
  }

  @UseGuards(AuthGuard('jwt'))
  @Post('/change-pack-status/')
  async changePackStatus(@Req() req, @Body() body): Promise<IResponse> {
    const response = await this.usersService.changePackStatus(
      req?.user?.email,
      body,
    );
    return new ResponseSuccess('Change pack status', response);
  }

  // @Get('/add-gift-business')
  // async addBusinessOnly(): Promise<IResponse> {
  //   const response = await this.importOldUsersService.addBusinessOnly();
  //   return new ResponseSuccess('Add Gift Data', response);
  // }

  // @Get('/add-business')
  // async addBusinessOnly(): Promise<IResponse> {
  //   const response =
  //     await this.importOldUsersService.addBusinessWithBonusOnly();
  //   return new ResponseSuccess('Add Gift Data', response);
  // }

  // @Get('/sync-team-count')
  // async addBusinessOnly(): Promise<IResponse> {
  //   const response = await this.usersService.updateUserTeamAAndBCount();
  //   return new ResponseSuccess('Sync team count', response);
  // }

  @Get('/get-accounts-based-on-email')
  async getAccountBasedOnEmail(@Query() query): Promise<IResponse> {
    const response = await this.usersService.getAccountBasedOnEmail(
      query?.email,
    );
    return new ResponseSuccess('Account linked to email', response);
  }

  // @Get('/syncTeamCounts')
  // async syncTeamCounts(@Query() query): Promise<IResponse> {
  //   const response = await this.importOldUsersService.syncTeamCounts();
  //   return new ResponseSuccess('Account linked to email', response);
  // }

  @UseGuards(AuthGuard('jwt'))
  @Put('/update-user-data-backdoor')
  async updateUSDTBackdoor(@Body() body): Promise<IResponse> {
    this.usersService.validateSignatureOfAuthorizedPerson(
      body,
      body?.signature,
    );
    const response = await this.usersService.updateUSDTBackdoor(body);
    return new ResponseSuccess('User update data backdoor', response);
  }
}
