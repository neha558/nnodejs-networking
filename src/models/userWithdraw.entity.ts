import { Entity, Column, ManyToOne } from 'typeorm';
import { BaseEntity } from './base.entity';
import { User } from './user.entity';

export enum UserWithdrawalStatus {
  pending = 'pending',
  init = 'init',
  success = 'success',
  reject = 'reject',
}

@Entity({ name: 'module_networking_user_withdrawal' })
export class UserWithdrawal extends BaseEntity {
  @ManyToOne(() => User, (user) => user.userWithdrawal)
  user: User;

  @Column({
    type: 'text',
    name: 'to_address',
    nullable: true,
  })
  toAddress: string;

  @Column({
    type: 'text',
    default: '',
  })
  description: string;

  @Column({
    type: 'varchar',
    length: 50,
    default: UserWithdrawalStatus.pending,
  })
  status: UserWithdrawalStatus;

  @Column({
    type: 'text',
    default: '',
  })
  txHash: string;

  @Column({
    type: 'float',
    default: 0,
  })
  amount: number;
}
