import { Entity, Column, ManyToOne } from 'typeorm';
import { BaseEntity } from './base.entity';
import { User } from './user.entity';

@Entity({ name: 'module_networking_ref_profits' })
export class RefProfit extends BaseEntity {
  @ManyToOne(() => User, (user) => user.myProfit)
  user: User;

  @ManyToOne(() => User, (user) => user.profitedFromMe)
  profitFromUser: User;

  @Column({
    type: 'int',
  })
  level: number;

  @Column({
    type: 'int',
    default: 0,
  })
  incomeType: number;

  @Column({
    type: 'float',
    default: 0,
  })
  totalIncome: number;

  @Column({
    type: 'float',
  })
  profit: number;

  @Column({
    type: 'float',
  })
  percentage: number;
}
