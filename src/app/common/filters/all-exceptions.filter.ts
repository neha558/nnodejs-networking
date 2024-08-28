import {
  ExceptionFilter,
  Catch,
  ArgumentsHost,
  HttpStatus,
  BadRequestException,
} from '@nestjs/common';
import { HttpException } from '@nestjs/common';
import { EntityNotFoundError } from 'typeorm';

@Catch()
export class AllExceptionsFilter implements ExceptionFilter {
  catch(exception: Error, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse();

    let status =
      exception instanceof HttpException
        ? exception.getStatus()
        : HttpStatus.INTERNAL_SERVER_ERROR;

    let exceptionRes: any =
      exception instanceof BadRequestException
        ? exception.getResponse()
        : {
            message: exception.message,
          };

    if (exception instanceof EntityNotFoundError) {
      status = HttpStatus.NOT_FOUND;
      exceptionRes = {
        message: 'No Data found',
      };
    }

    console.error(exception);

    response.status(status).json({
      status: false,
      code: status,
      message: Array.isArray(exceptionRes.message)
        ? exceptionRes.message[0]
        : exceptionRes.message,
      data: null,
    });
  }
}
