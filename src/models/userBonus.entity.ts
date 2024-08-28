import { Entity, Column, ManyToOne } from 'typeorm';
import { BaseEntity } from './base.entity';
import { User } from './user.entity';
import { PackBought } from './packBought.entity';

export enum BonusTypes {
  direct = 'direct',
  team = 'team',
  teamDirect = 'team_direct',
  rank = 'rank',
}

@Entity({ name: 'module_networking_user_bonus' })
export class UserBonus extends BaseEntity {
  @ManyToOne(() => User, (user) => user.bonus)
  user: User;

  @ManyToOne(() => User, (user) => user.bonusFrom)
  bonusFrom: User;

  @ManyToOne(() => PackBought, (pack) => pack.bonus)
  packBought: PackBought;

  @Column({
    type: 'float',
  })
  amount: number;

  @Column({
    type: 'varchar',
    length: 50,
    default: BonusTypes.direct,
  })
  bonusType: BonusTypes;

  @Column({
    type: 'int',
    default: 0,
  })
  userLevel: number;

  @Column({
    type: 'float',
    default: 0,
  })
  percentage: number;

  @Column({
    type: 'boolean',
    default: false,
  })
  canClaim: boolean;

  @Column({
    type: 'float',
  })
  flushAmount: number;

  @Column({
    type: 'boolean',
    default: false,
  })
  onHold: boolean;
}
