import { IsNotEmpty, IsString, IsOptional, IsEmail } from 'class-validator';
import { AppStrings } from 'src/app/common/constants/strings';

export class UserRegisterDTO {
  @IsOptional({
    message: AppStrings?.UserRegisterDTO?.accountAddress?.IsNotEmpty,
  })
  @IsString({
    message: AppStrings?.UserRegisterDTO?.accountAddress?.IsString,
  })
  accountAddress: string;

  @IsOptional()
  @IsString({
    message: AppStrings?.UserRegisterDTO?.userName?.IsString,
  })
  userName: string;

  @IsString({
    message: AppStrings?.UserRegisterDTO?.email?.IsString,
  })
  @IsEmail(
    {},
    {
      message: AppStrings?.UserRegisterDTO?.email?.IsEmail,
    },
  )
  email: string;

  @IsOptional()
  referredBy: string;

  @IsOptional()
  parent: string;

  @IsOptional()
  code: string;

  @IsOptional()
  team: string;
}
