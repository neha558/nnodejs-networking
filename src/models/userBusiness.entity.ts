import { Entity, Column } from 'typeorm';
import { BaseEntity } from './base.entity';

@Entity({ name: 'module_networking_user_business' })
export class UserBusiness extends BaseEntity {
  @Column({
    type: 'int',
  })
  node: number; // team

  @Column({
    type: 'text',
    name: 'account_address',
  })
  accountAddress: string;

  @Column({
    type: 'text',
    name: 'joined_account_address',
  })
  joinedByAccountAddress: string;

  @Column({
    type: 'float',
    default: 0,
  })
  businessIncome: number;
}
