import { Entity, Column, OneToMany, ManyToOne } from 'typeorm';
import { BaseEntity } from './base.entity';
import { RefProfit } from './refProfit.entity';
import { Rank } from './rank.entity';
import { PackBought } from './packBought.entity';
import { UserBonus } from './userBonus.entity';
import { UserActivity } from './userActivity.entity';
import { UserWithdrawal } from './userWithdraw.entity';

@Entity({ name: 'module_networking_users' })
export class User extends BaseEntity {
  @Column({
    type: 'text',
    name: 'account_address',
    unique: true,
  })
  accountAddress: string;

  @Column({
    type: 'text',
    name: 'wallet_address',
    unique: true,
    nullable: true,
  })
  walletAddress: string;

  @Column({
    type: 'text',
    name: 'referral_code',
    unique: true,
  })
  referralCode: string;

  @Column({
    type: 'text',
    name: 'referral_code_seconds',
    unique: true,
  })
  referralCodeSecond: string;

  @Column({
    type: 'int',
  })
  treeDepth: number;

  @Column({
    type: 'int',
  })
  node: number; // team

  @Column({
    type: 'int',
  })
  level: number;

  @Column({
    type: 'float',
    default: 0,
  })
  individualIncome: number;

  @Column({
    type: 'float',
    default: 0,
  })
  businessIncome: number;

  @Column({
    type: 'float',
    default: 0,
  })
  businessIncomeFirstTeam: number;

  @Column({
    type: 'float',
    default: 0,
  })
  businessIncomeSecondTeam: number;

  @Column({
    type: 'float',
    default: 0,
  })
  businessIncomeFirstDeltaTeam: number;

  @Column({
    type: 'float',
    default: 0,
  })
  businessIncomeSecondDeltaTeam: number;

  @Column({
    type: 'text',
    default: '',
  })
  starRecords: string; // can save like {startId}_{totalStat} // 1_2 | 2_1

  @Column({
    type: 'boolean',
    default: false,
  })
  acceptedTerms: boolean;

  @Column({
    type: 'text',
    name: 'username',
    unique: true,
    nullable: true,
  })
  userName: string;

  @Column({
    type: 'text',
    name: 'email',
    nullable: true,
  })
  email: string;

  @Column({
    type: 'text',
    name: 'password',
    nullable: true,
  })
  password: string;

  @Column({
    type: 'text',
    name: 'token',
    nullable: true,
  })
  token: string;

  @ManyToOne(() => User, (user) => user.myReferrals)
  referredBy: User;

  @OneToMany(() => User, (user) => user.referredBy)
  myReferrals: User[];

  @ManyToOne(() => User, (user) => user.children)
  parent: User;

  @OneToMany(() => User, (user) => user.parent)
  children: User[];

  @OneToMany(() => RefProfit, (refProfit) => refProfit.user)
  myProfit: RefProfit[];

  @OneToMany(() => RefProfit, (refProfit) => refProfit.profitFromUser)
  profitedFromMe: RefProfit[];

  @ManyToOne(() => Rank, (rank) => rank.users)
  profile: Rank;

  @OneToMany(() => PackBought, (packBought) => packBought.user)
  packBought: PackBought[];

  @OneToMany(() => UserBonus, (userBonus) => userBonus.user)
  bonus: UserBonus[];

  @OneToMany(() => UserBonus, (userBonus) => userBonus.bonusFrom)
  bonusFrom: UserBonus[];

  @Column('simple-array')
  parents: string[];

  @Column('simple-array')
  sponsorTree: string[];

  @Column({
    type: 'float',
    default: 0,
  })
  directSponsorBonus: number;

  @Column({
    type: 'float',
    default: 0,
  })
  teamMatchingBonus: number;

  @Column({
    type: 'float',
    default: 0,
  })
  directMatchingBonus: number;

  @Column({
    type: 'float',
    default: 0,
  })
  rankBonus: number;

  @Column({
    type: 'float',
    default: 0,
  })
  infinityPoolBonus: number;

  @Column({
    type: 'float',
    default: 0,
  })
  presidentBonus: number;

  @OneToMany(() => UserActivity, (userActivity) => userActivity.user)
  userActivities: UserActivity[];

  @OneToMany(() => UserWithdrawal, (userWithdrawal) => userWithdrawal.user)
  userWithdrawal: UserWithdrawal[];

  @Column({
    type: 'int',
    default: 0,
  })
  teamACount: number;

  @Column({
    type: 'int',
    default: 0,
  })
  teamBCount: number;

  @Column({
    type: 'int',
    default: 0,
  })
  directPartner: number;

  @Column({
    type: 'float',
    default: 0,
  })
  totalWithdrawableAmount: number;

  @Column({
    type: 'int',
    default: 0,
  })
  lastBoughtPack: number;

  @Column({
    type: 'int',
    default: 0,
  })
  lastBoughtPackPrice: number;

  @Column({
    type: 'int',
    default: 0,
  })
  rankBusinessATeamBucket: number;

  @Column({
    type: 'int',
    default: 0,
  })
  rankBusinessBTeamBucket: number;

  @Column({
    type: 'boolean',
    default: false,
  })
  legacyUsers: boolean;
}
