import { Entity, Column, ManyToOne, OneToMany, Generated } from 'typeorm';
import { BaseEntity } from './base.entity';
import { User } from './user.entity';
import { Pack } from './pack.entity';
import { UserBonus } from './userBonus.entity';

export enum PackBoughtStatus {
  paid = 'paid',
  nftDistributed = 'nft_distributed',
  rejected = 'rejected',
  initiated = 'initiated',
}

@Entity({ name: 'module_networking_pack_bought' })
export class PackBought extends BaseEntity {
  @Column()
  _id: number;

  @ManyToOne(() => User, (user) => user.packBought)
  user: User;

  @ManyToOne(() => Pack, (pack) => pack.packBought)
  pack: Pack;

  @Column({
    type: 'float',
  })
  packPrice: number;

  @Column({
    type: 'text',
    nullable: true,
  })
  txHash: string;

  @Column({
    type: 'text',
  })
  blockChainData: string;

  @Column({
    type: 'varchar',
    length: 50,
    default: PackBoughtStatus.paid,
  })
  status: PackBoughtStatus;

  @OneToMany(() => UserBonus, (userBonus) => userBonus.packBought)
  bonus: UserBonus[];
}
