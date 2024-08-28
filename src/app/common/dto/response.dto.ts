import { Logger } from '@nestjs/common';
import { IResponse } from '../interfaces/response.interface';

export class ResponseError implements IResponse {
  status: boolean;
  code: string | number;
  message: string;
  data: any[];

  constructor(infoMessage: string, data?: any, code = 500) {
    this.status = false;
    this.code = code;
    this.message = infoMessage;
    this.data = data;
    Logger.error(
      new Date().toString() +
        ' - [Response]: ' +
        infoMessage +
        (data ? ' - ' + JSON.stringify(data) : ''),
    );
  }
}

export class ResponseSuccess implements IResponse {
  status: boolean;
  code: string | number;
  message: string;
  data: any[];
  constructor(message: string, data?: any, code = 200) {
    this.status = true;
    this.code = code;
    this.message = message;
    this.data = data;
  }
}
