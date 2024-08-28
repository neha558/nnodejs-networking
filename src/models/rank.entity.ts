import { Entity, Column, OneToMany } from 'typeorm';
import { BaseEntity } from './base.entity';
import { User } from './user.entity';

@Entity({ name: 'module_networking_ranks' })
export class Rank extends BaseEntity {
  @Column({
    type: 'int',
    unique: true,
  })
  _id: number;

  @Column({
    type: 'text',
    unique: true,
  })
  name: string;

  @Column({
    type: 'text',
    nullable: true,
  })
  desc: string;

  @Column({
    type: 'text',
    nullable: true,
  })
  image: string;

  @Column({
    type: 'text',
    nullable: true,
  })
  color: string;

  @Column({
    type: 'int',
  })
  minimumBusinessRequired: number;

  @Column({
    type: 'text',
  })
  minimumStarWithRank: string; // {no_of_start}_{_id_of_rank} | {no_of_start}_{_id_of_rank} // can be saved and write logic on it

  @Column({
    type: 'text',
  })
  matchingRatio: string; // 1:1, 1:3

  @Column({
    type: 'float',
    nullable: true,
  })
  directSponsorBonus: number;

  @Column({
    type: 'float',
    nullable: true,
  })
  rankBonus: number;

  @OneToMany(() => User, (user) => user.profile)
  users: User[];

  @Column({
    type: 'int',
  })
  directSponsorRequired: number;
}
